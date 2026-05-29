"""Giới hạn tần suất quét Insights — tránh checkpoint Meta."""

from __future__ import annotations

import os
import random
import time
from dataclasses import dataclass
from typing import Any

from src.services.facebook_session_recovery import facebook_page_is_hard_checkpoint
from src.utils.page_insights_store import PageInsightsPeriod, PageInsightsStore


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name, "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class PageInsightsPolicy:
    """Ngưỡng mặc định an toàn — chỉnh qua biến môi trường."""

    min_interval_hours: float
    min_retry_error_hours: float
    max_pages_per_run: int
    account_cooldown_min: int
    page_delay_min_sec: float
    page_delay_max_sec: float
    url_delay_min_sec: float
    url_delay_max_sec: float
    force_refresh: bool

    @classmethod
    def from_env(cls) -> PageInsightsPolicy:
        return cls(
            min_interval_hours=max(1.0, float(_env_int("TOOLFB_PAGE_INSIGHTS_MIN_INTERVAL_HOURS", 12))),
            min_retry_error_hours=max(0.5, float(_env_int("TOOLFB_PAGE_INSIGHTS_RETRY_ERROR_HOURS", 2))),
            max_pages_per_run=max(1, _env_int("TOOLFB_PAGE_INSIGHTS_MAX_PER_RUN", 3)),
            account_cooldown_min=max(0, _env_int("TOOLFB_PAGE_INSIGHTS_ACCOUNT_COOLDOWN_MIN", 45)),
            page_delay_min_sec=max(5.0, float(_env_int("TOOLFB_PAGE_INSIGHTS_PAGE_DELAY_MIN_SEC", 45))),
            page_delay_max_sec=max(5.0, float(_env_int("TOOLFB_PAGE_INSIGHTS_PAGE_DELAY_MAX_SEC", 90))),
            url_delay_min_sec=max(1.0, float(_env_int("TOOLFB_PAGE_INSIGHTS_URL_DELAY_MIN_SEC", 2))),
            url_delay_max_sec=max(1.0, float(_env_int("TOOLFB_PAGE_INSIGHTS_URL_DELAY_MAX_SEC", 5))),
            force_refresh=_env_bool("TOOLFB_PAGE_INSIGHTS_FORCE", False),
        )


@dataclass
class PageInsightsFetchPlan:
    to_fetch: list[dict[str, Any]]
    skipped: list[tuple[str, str]]
    deferred_over_limit: list[str]
    account_blocked: dict[str, str]


def plan_page_insights_fetch(
    pages: list[dict[str, Any]],
    *,
    period: PageInsightsPeriod,
    store: PageInsightsStore,
    policy: PageInsightsPolicy | None = None,
    force_refresh: bool = False,
) -> PageInsightsFetchPlan:
    pol = policy or PageInsightsPolicy.from_env()
    force = force_refresh or pol.force_refresh
    skipped: list[tuple[str, str]] = []
    candidates: list[dict[str, Any]] = []
    account_blocked: dict[str, str] = {}

    for row in pages:
        pid = str(row.get("id", "")).strip()
        if not pid:
            continue
        skip, reason = store.should_skip_fetch(
            pid,
            period,
            min_hours_success=pol.min_interval_hours,
            min_hours_error=pol.min_retry_error_hours,
            force=force,
        )
        if skip:
            skipped.append((pid, reason))
            continue
        candidates.append(row)

    to_fetch: list[dict[str, Any]] = []
    deferred: list[str] = []
    for row in candidates:
        aid = str(row.get("account_id", "")).strip()
        if aid and aid not in account_blocked:
            ok, msg = store.account_can_start_session(aid, pol.account_cooldown_min)
            if not ok:
                account_blocked[aid] = msg
        if aid and aid in account_blocked:
            deferred.append(str(row.get("id", "")))
            continue
        if len(to_fetch) >= pol.max_pages_per_run:
            deferred.append(str(row.get("id", "")))
            continue
        to_fetch.append(row)

    return PageInsightsFetchPlan(
        to_fetch=to_fetch,
        skipped=skipped,
        deferred_over_limit=deferred,
        account_blocked=account_blocked,
    )


def sleep_page_gap(policy: PageInsightsPolicy | None = None) -> None:
    pol = policy or PageInsightsPolicy.from_env()
    lo = min(pol.page_delay_min_sec, pol.page_delay_max_sec)
    hi = max(pol.page_delay_min_sec, pol.page_delay_max_sec)
    time.sleep(random.uniform(lo, hi))


def sleep_url_gap(policy: PageInsightsPolicy | None = None) -> None:
    pol = policy or PageInsightsPolicy.from_env()
    lo = min(pol.url_delay_min_sec, pol.url_delay_max_sec)
    hi = max(pol.url_delay_min_sec, pol.url_delay_max_sec)
    time.sleep(random.uniform(lo, hi))


def insights_page_is_blocked(page_url: str, body_text: str = "") -> tuple[bool, str]:
    u = str(page_url or "").strip()
    if facebook_page_is_hard_checkpoint(u):
        return True, "Meta checkpoint / xác minh — dừng quét thống kê."
    low = (body_text or "").lower()
    if any(x in low for x in ("confirm your identity", "xác minh danh tính", "unusual activity")):
        return True, "Meta yêu cầu xác minh — dừng quét thống kê."
    if "/login" in u.lower() and "facebook.com" in u.lower():
        return True, "Phiên đăng nhập hết hạn — dừng quét thống kê."
    return False, ""
