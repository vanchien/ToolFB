"""
Pipeline 3 tầng giải captcha checkpoint Facebook (Anti-block).

Mặc định (``captcha_prefer_twocaptcha: true``):
  Tầng 1: 2Captcha + proxy tài khoản
  Tầng 2: 2Captcha ProxyLess
  Tầng 3: CapSolver + proxy (fallback)

→ Thất bại: captcha thủ công 180s (giữ profile Firefox).
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from loguru import logger
from playwright.sync_api import Page

from src.services.capsolver_client import CapSolverError, solve_recaptcha_v2
from src.services.facebook_recaptcha import (
    _build_capsolver_attempts,
    _build_hybrid_proxyless_attempts,
    _capsolver_hybrid_proxyless_enabled,
    _capsolver_tier_disabled,
    _capsolver_use_account_proxy,
    facebook_recaptcha_task_urls,
)
from src.services.twocaptcha_client import TwoCaptchaError, solve_recaptcha_v2_enterprise
from src.utils.capsolver_config import capsolver_auto_solve_enabled, get_capsolver_api_key
from src.utils.twocaptcha_config import (
    captcha_enterprise_tier_timeout_sec,
    captcha_prefer_twocaptcha_first,
    captcha_tier_timeout_sec,
    get_twocaptcha_api_key,
    twocaptcha_has_sufficient_balance,
)

_UNSOLVABLE_MARKERS = ("ERROR_CAPTCHA_UNSOLVABLE", "CAPTCHA_UNSOLVABLE", "UNSOLVABLE")


@dataclass(frozen=True)
class CaptchaTierResult:
    """Kết quả giải tự động — token + metadata inject."""

    solution: dict[str, Any]
    provider: str
    proxyless: bool


_PROXY_FAIL_MARKERS = (
    "ERROR_PROXY_CONNECT_REFUSED",
    "ERROR_PROXY_CONNECT",
    "ERROR_INVALID_PROXY",
    "ERROR_PROXY_FORMAT",
    "ERROR_PROXY_CONNECT_TIMEOUT",
    "custom proxy connect failed",
    "CONNECT_REFUSED",
)


def _tier_urls_for_task(page: Page, page_url: str) -> list[str]:
    """URL canonical cho task API — theo host/path trang hiện tại (m/www)."""
    return facebook_recaptcha_task_urls(page, page_url=page_url)


def _run_twocaptcha_tier(
    page: Page,
    solve_kwargs: dict[str, Any],
    *,
    page_url: str,
    use_proxy: bool,
    tier_timeout_sec: float,
    should_stop: Callable[[], bool] | None,
    tier_label: str,
) -> CaptchaTierResult | None:
    """Một tầng 2Captcha (proxy hoặc ProxyLess)."""
    if should_stop and should_stop():
        return None
    api_key = get_twocaptcha_api_key()
    if not api_key:
        return None
    if not twocaptcha_has_sufficient_balance():
        logger.warning(
            "[FB captcha pipeline] 2Captcha số dư = 0 — bỏ qua {}.",
            tier_label,
        )
        return None

    proxy_config = solve_kwargs.get("proxy_config") if use_proxy else None
    if use_proxy and not proxy_config:
        logger.warning("[FB captcha pipeline] {} 2Captcha: thiếu proxy — bỏ qua.", tier_label)
        return None

    urls = _tier_urls_for_task(page, str(page_url or solve_kwargs.get("website_url") or ""))
    last_exc: Exception | None = None
    tag = "twocaptcha_proxy" if use_proxy else "twocaptcha_proxyless"

    for url in urls:
        if should_stop and should_stop():
            return None
        try:
            sol = solve_recaptcha_v2_enterprise(
                website_url=url,
                website_key=str(solve_kwargs["website_key"]),
                api_key=api_key,
                proxy_config=proxy_config if use_proxy else None,
                recaptcha_data_s_value=str(solve_kwargs.get("recaptcha_data_s_value") or ""),
                user_agent=str(solve_kwargs.get("user_agent") or ""),
                page_action=str(solve_kwargs.get("page_action") or ""),
                is_invisible=bool(solve_kwargs.get("is_invisible")),
                timeout_sec=tier_timeout_sec,
            )
            logger.info("[FB captcha pipeline] {} thành công — {}.", tier_label, tag)
            return CaptchaTierResult(solution=sol, provider=tag, proxyless=not use_proxy)
        except TwoCaptchaError as exc:
            last_exc = exc
            msg = str(exc)
            if "ERROR_ZERO_BALANCE" in msg:
                logger.warning("[FB captcha pipeline] 2Captcha ERROR_ZERO_BALANCE — nhảy tầng sau / captcha tay.")
                return None
            if any(m in msg.upper() for m in _UNSOLVABLE_MARKERS):
                logger.warning(
                    "[FB captcha pipeline] {} {} UNSOLVABLE — bỏ URL còn lại, nhảy tầng sau.",
                    tier_label,
                    tag,
                )
                break
            logger.info("[FB captcha pipeline] {} {} url={}: {}", tier_label, tag, url[:60], msg[:120])
    if last_exc:
        logger.info("[FB captcha pipeline] {} thất bại: {}", tier_label, str(last_exc)[:120])
    return None


def _run_twocaptcha_tiers(
    page: Page,
    solve_kwargs: dict[str, Any],
    *,
    page_url: str,
    tier_timeout_sec: float,
    should_stop: Callable[[], bool] | None,
) -> CaptchaTierResult | None:
    """Tầng 1–2: 2Captcha proxy rồi ProxyLess."""
    if not get_twocaptcha_api_key():
        return None
    t0 = time.monotonic()
    r1 = _run_twocaptcha_tier(
        page,
        solve_kwargs,
        page_url=page_url,
        use_proxy=True,
        tier_timeout_sec=tier_timeout_sec,
        should_stop=should_stop,
        tier_label="Tầng 1 (2Captcha+proxy)",
    )
    if r1:
        logger.info(
            "[FB captcha pipeline] Hoàn tất tầng 1 2Captcha ({:.1f}s).",
            time.monotonic() - t0,
        )
        return r1
    t0 = time.monotonic()
    r2 = _run_twocaptcha_tier(
        page,
        solve_kwargs,
        page_url=page_url,
        use_proxy=False,
        tier_timeout_sec=tier_timeout_sec,
        should_stop=should_stop,
        tier_label="Tầng 2 (2Captcha ProxyLess)",
    )
    if r2:
        logger.info(
            "[FB captcha pipeline] Hoàn tất tầng 2 2Captcha ProxyLess ({:.1f}s).",
            time.monotonic() - t0,
        )
        return r2
    return None


def _run_capsolver_tier(
    solve_kwargs: dict[str, Any],
    *,
    api_key: str,
    account: dict[str, Any] | None,
    page_url: str,
    tier_timeout_sec: float,
    should_stop: Callable[[], bool] | None,
    tier_label: str = "Tầng 3 (CapSolver+proxy)",
) -> CaptchaTierResult | None:
    """CapSolver Enterprise + proxy (fallback ProxyLess CapSolver nếu CONNECT_REFUSED)."""
    proxy_config = solve_kwargs.get("proxy_config")
    if not proxy_config and _capsolver_use_account_proxy(account, page_url=page_url):
        logger.warning("[FB captcha pipeline] {}: thiếu proxy tài khoản — bỏ qua.", tier_label)
        return None

    attempts = _build_capsolver_attempts(dict(solve_kwargs), prefer_enterprise=True)
    if not attempts:
        return None

    last_exc: Exception | None = None
    skip_proxy = False
    domain_reject = 0
    total = 0

    def _try_list(attempt_list: list[dict[str, Any]]) -> tuple[dict[str, Any], bool] | None:
        nonlocal last_exc, skip_proxy, domain_reject, total
        for args in attempt_list:
            if should_stop and should_stop():
                return None
            if skip_proxy and (args.get("proxy") or args.get("proxy_config")):
                continue
            total += 1
            run_args = dict(args)
            run_args.pop("_account", None)
            run_args["api_key"] = api_key
            run_args["proxy"] = None
            run_args["timeout_sec"] = tier_timeout_sec
            run_args["poll_interval_sec"] = 2.0
            if not run_args.get("proxy_config"):
                run_args.pop("proxy_config", None)
            try:
                from src.services.capsolver_client import capsolver_website_url_for_task

                run_args["website_url"] = capsolver_website_url_for_task(
                    str(run_args.get("website_url") or ""),
                    proxyless=not bool(run_args.get("proxy_config")),
                )
                sol = solve_recaptcha_v2(**run_args)
                proxyless = not bool(args.get("proxy") or args.get("proxy_config"))
                return sol, proxyless
            except CapSolverError as exc:
                last_exc = exc
                msg = str(exc)
                if any(m in msg for m in _PROXY_FAIL_MARKERS):
                    skip_proxy = True
                    logger.warning(
                        "[FB captcha pipeline] {} CONNECT_REFUSED — thử CapSolver ProxyLess.",
                        tier_label,
                    )
                    continue
                if "Invalid domain for site key" in msg:
                    domain_reject += 1
                logger.info("[FB captcha pipeline] {}: {}", tier_label, msg[:140])
        return None

    out = _try_list(attempts[:2])
    if out:
        sol, pl = out
        return CaptchaTierResult(solution=sol, provider="capsolver", proxyless=pl)

    if (
        skip_proxy
        and _capsolver_hybrid_proxyless_enabled()
        and _capsolver_use_account_proxy(account, page_url=page_url)
    ):
        pl_attempts = _build_hybrid_proxyless_attempts(dict(solve_kwargs), prefer_enterprise=True)
        logger.info(
            "[FB captcha pipeline] {}b CapSolver ProxyLess | {} URL.",
            tier_label,
            len(pl_attempts),
        )
        out2 = _try_list(pl_attempts[:3])
        if out2:
            sol, _ = out2
            return CaptchaTierResult(solution=sol, provider="capsolver_proxyless", proxyless=True)

    if domain_reject >= total and total > 0:
        logger.info("[FB captcha pipeline] {}: sitekey/domain Meta bị từ chối.", tier_label)
    elif last_exc:
        logger.info("[FB captcha pipeline] {} hết — {}", tier_label, str(last_exc)[:120])
    return None


def _run_capsolver_fallback(
    page: Page,
    solve_kwargs: dict[str, Any],
    account: dict[str, Any] | None,
    *,
    page_url: str,
    tier_timeout_sec: float,
    should_stop: Callable[[], bool] | None,
) -> CaptchaTierResult | None:
    if _capsolver_tier_disabled(page, account):
        logger.info("[FB captcha pipeline] CapSolver bỏ qua — tầng dự phòng tắt / hard-fail / skip Meta.")
        return None
    if not capsolver_auto_solve_enabled():
        return None
    api_key = get_capsolver_api_key()
    if not api_key:
        logger.info("[FB captcha pipeline] CapSolver bỏ qua — chưa có API key.")
        return None
    t0 = time.monotonic()
    r = _run_capsolver_tier(
        solve_kwargs,
        api_key=api_key,
        account=account,
        page_url=page_url,
        tier_timeout_sec=tier_timeout_sec,
        should_stop=should_stop,
        tier_label="Tầng 3 (CapSolver+proxy)",
    )
    if r:
        logger.info(
            "[FB captcha pipeline] Hoàn tất tầng 3 CapSolver ({:.1f}s) provider={}.",
            time.monotonic() - t0,
            r.provider,
        )
    return r


def run_anti_block_captcha_pipeline(
    page: Page,
    solve_kwargs: dict[str, Any],
    account: dict[str, Any] | None,
    *,
    page_url: str,
    should_stop: Callable[[], bool] | None = None,
) -> CaptchaTierResult | None:
    """
    Chạy 3 tầng anti-block. Mặc định 2Captcha trước, CapSolver dự phòng.
    """
    tier_timeout = captcha_tier_timeout_sec()
    sitekey = str(solve_kwargs.get("website_key") or "")
    if sitekey.startswith("6Le"):
        tier_timeout = captcha_enterprise_tier_timeout_sec()
    prefer_2c = captcha_prefer_twocaptcha_first()
    order = "2Captcha→CapSolver" if prefer_2c else "CapSolver→2Captcha"
    logger.info(
        "[FB captcha pipeline] Anti-block ({}) | timeout/tầng={:.0f}s | sitekey={}",
        order,
        tier_timeout,
        str(solve_kwargs.get("website_key", ""))[:12],
    )

    if prefer_2c:
        if get_twocaptcha_api_key():
            r = _run_twocaptcha_tiers(
                page,
                solve_kwargs,
                page_url=page_url,
                tier_timeout_sec=tier_timeout,
                should_stop=should_stop,
            )
            if r:
                return r
        else:
            logger.info("[FB captcha pipeline] Thiếu twocaptcha_api_key — chỉ thử CapSolver.")
        r_cap = _run_capsolver_fallback(
            page,
            solve_kwargs,
            account,
            page_url=page_url,
            tier_timeout_sec=tier_timeout,
            should_stop=should_stop,
        )
        if r_cap:
            return r_cap
    else:
        r_cap = _run_capsolver_fallback(
            page,
            solve_kwargs,
            account,
            page_url=page_url,
            tier_timeout_sec=tier_timeout,
            should_stop=should_stop,
        )
        if r_cap:
            return r_cap
        if get_twocaptcha_api_key():
            r = _run_twocaptcha_tiers(
                page,
                solve_kwargs,
                page_url=page_url,
                tier_timeout_sec=tier_timeout,
                should_stop=should_stop,
            )
            if r:
                return r

    logger.warning(
        "[FB captcha pipeline] Hết tầng tự động — captcha THỦ CÔNG (Firefox ~180s)."
    )
    return None
