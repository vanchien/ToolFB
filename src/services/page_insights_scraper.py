"""
Lấy followers + views từ Meta Business Insights (Playwright, cookie tài khoản owner).

Giới hạn tần suất: xem ``page_insights_policy`` và biến ``TOOLFB_PAGE_INSIGHTS_*``.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from loguru import logger
from playwright.sync_api import BrowserContext, Page, TimeoutError as PlaywrightTimeoutError

from src.automation.facebook_actions import (
    login_with_cookie,
    page_row_facebook_asset_id,
    prime_facebook_session_page,
)
from src.services.page_insights_parse import (
    _EXTRACT_METRICS_JS,
    merge_metrics,
    metrics_from_body_text,
)
from src.utils.page_insights_policy import (
    PageInsightsPolicy,
    insights_page_is_blocked,
    sleep_page_gap,
    sleep_url_gap,
)
from src.utils.page_insights_store import PageInsightsPeriod, PageInsightsSnapshot

StatusCallback = Callable[[str], None]

_PERIOD_BUTTON_PATTERNS: dict[PageInsightsPeriod, tuple[re.Pattern[str], ...]] = {
    "7d": (
        re.compile(r"last\s*7\s*days?", re.I),
        re.compile(r"7\s*days?", re.I),
        re.compile(r"7\s*ngày", re.I),
        re.compile(r"^7d$", re.I),
    ),
    "28d": (
        re.compile(r"last\s*28\s*days?", re.I),
        re.compile(r"28\s*days?", re.I),
        re.compile(r"28\s*ngày", re.I),
        re.compile(r"tháng", re.I),
        re.compile(r"month", re.I),
    ),
}

_OVERVIEW_URL = "https://business.facebook.com/latest/insights/overview?asset_id={asset_id}"
_PEOPLE_URL = "https://business.facebook.com/latest/insights/people?asset_id={asset_id}"
_CONTENT_URL = "https://business.facebook.com/latest/insights/content?asset_id={asset_id}"


@dataclass
class PageInsightsBatchResult:
    results: list[tuple[str, PageInsightsSnapshot]] = field(default_factory=list)
    stopped_early: bool = False
    stop_reason: str = ""


def _numeric_id_from_url(url: str) -> str:
    u = str(url or "").strip()
    if not u:
        return ""
    for pat in (
        r"[?&]id=(\d{8,})",
        r"/pages/[^/]+/(\d{8,})",
        r"/(\d{8,})(?:[/?#]|$)",
    ):
        m = re.search(pat, u, flags=re.I)
        if m:
            return m.group(1)
    return ""


def resolve_page_asset_id(page_row: dict[str, Any]) -> str:
    aid = page_row_facebook_asset_id(page_row) or ""
    if aid:
        return str(aid).strip()
    return _numeric_id_from_url(str(page_row.get("page_url", "")))


def _status(cb: StatusCallback | None, msg: str) -> None:
    if cb:
        try:
            cb(msg)
        except Exception:
            pass
    logger.info("[page_insights] {}", msg)


def _body_snippet(page: Page) -> str:
    try:
        return (page.locator("body").inner_text(timeout=4_000) or "")[:4000]
    except Exception:
        return ""


def _assert_page_safe(page: Page) -> None:
    blocked, reason = insights_page_is_blocked(str(page.url or ""), _body_snippet(page))
    if blocked:
        raise RuntimeError(reason)


def _select_insights_period(page: Page, period: PageInsightsPeriod) -> bool:
    patterns = _PERIOD_BUTTON_PATTERNS.get(period, ())
    for pat in patterns:
        for role in ("button", "tab", "option", "menuitem"):
            try:
                loc = page.get_by_role(role, name=pat)
                if loc.count() > 0:
                    loc.first.click(timeout=4_000)
                    page.wait_for_timeout(700)
                    return True
            except Exception:
                continue
        try:
            loc = page.get_by_text(pat)
            if loc.count() > 0:
                loc.first.click(timeout=4_000)
                page.wait_for_timeout(700)
                return True
        except Exception:
            continue
    return False


def _extract_on_page(page: Page) -> dict[str, Any]:
    try:
        raw = page.evaluate(_EXTRACT_METRICS_JS)
        if isinstance(raw, dict):
            return raw
    except Exception as exc:
        logger.debug("evaluate insights metrics: {}", exc)
    f, v = metrics_from_body_text(_body_snippet(page))
    return {"followers": f, "views": v, "url": str(page.url or "")}


def _scrape_one_url(
    page: Page,
    url: str,
    period: PageInsightsPeriod,
    *,
    policy: PageInsightsPolicy,
) -> dict[str, Any]:
    page.goto(url, wait_until="domcontentloaded", timeout=90_000)
    page.wait_for_timeout(1_800)
    _assert_page_safe(page)
    _select_insights_period(page, period)
    page.wait_for_timeout(1_100)
    return _extract_on_page(page)


def _urls_to_try(asset_id: str, followers: int | None, views: int | None) -> list[str]:
    """Tối đa 2 URL / Page — tránh quét 4 trang Insights liên tiếp."""
    urls = [_OVERVIEW_URL.format(asset_id=asset_id)]
    if followers is None:
        urls.append(_PEOPLE_URL.format(asset_id=asset_id))
    if views is None:
        urls.append(_CONTENT_URL.format(asset_id=asset_id))
    seen: set[str] = set()
    out: list[str] = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out[:2]


def scrape_page_insights_on_tab(
    pg: Page,
    *,
    page_row: dict[str, Any],
    period: PageInsightsPeriod,
    status_cb: StatusCallback | None = None,
    policy: PageInsightsPolicy | None = None,
) -> PageInsightsSnapshot:
    """Đọc Insights trên tab đã đăng nhập (không tạo tab mới)."""
    pol = policy or PageInsightsPolicy.from_env()
    asset_id = resolve_page_asset_id(page_row)
    page_id = str(page_row.get("id", "")).strip()
    if not asset_id:
        raise ValueError(
            f"Page {page_id or '(không id)'} thiếu Meta Page ID (fb_page_id) — không mở được Insights."
        )

    metrics_parts: list[dict[str, Any]] = []
    last_url = ""
    followers: int | None = None
    views: int | None = None

    for url in _urls_to_try(asset_id, followers, views):
        _status(status_cb, f"Insights: {page_row.get('page_name', page_id)} …")
        try:
            part = _scrape_one_url(pg, url, period, policy=pol)
            metrics_parts.append(part)
            last_url = str(part.get("url") or url)
            followers, views = merge_metrics(*metrics_parts)
            if followers is not None and views is not None:
                break
        except PlaywrightTimeoutError as exc:
            logger.warning("Insights timeout {}: {}", url, exc)
        except RuntimeError:
            raise
        except Exception as exc:
            logger.warning("Insights lỗi {}: {}", url, exc)
        sleep_url_gap(pol)

    followers, views = merge_metrics(*metrics_parts)
    err = ""
    if followers is None and views is None:
        err = "Không đọc được followers/views (UI Meta đổi hoặc thiếu quyền)."
    return {
        "period": period,
        "followers": followers,
        "views": views,
        "source_url": last_url,
        "error": err,
    }


def fetch_insights_for_pages(
    context: BrowserContext,
    *,
    account: dict[str, Any],
    pages: list[dict[str, Any]],
    period: PageInsightsPeriod,
    status_cb: StatusCallback | None = None,
    policy: PageInsightsPolicy | None = None,
) -> PageInsightsBatchResult:
    """
    Một tab / một lần đăng nhập cho cả nhóm Page (cùng owner).
    Nghỉ dài giữa các Page; dừng ngay khi gặp checkpoint.
    """
    pol = policy or PageInsightsPolicy.from_env()
    batch = PageInsightsBatchResult()
    if not pages:
        return batch

    cookie_path = str(account.get("cookie_path", "") or "").strip()
    if not cookie_path:
        raise ValueError("Tài khoản owner chưa có cookie_path — đăng nhập trước khi lấy thống kê.")

    aid = str(account.get("id", "") or "").strip()
    pg = context.new_page()
    total = len(pages)
    try:
        _status(status_cb, "Đăng nhập phiên Facebook (một lần cho nhóm Page)…")
        login_with_cookie(pg, cookie_path)
        prime_facebook_session_page(pg)
        _assert_page_safe(pg)

        for i, row in enumerate(pages, start=1):
            pid = str(row.get("id", "")).strip()
            name = str(row.get("page_name", "")).strip() or pid
            if i > 1:
                _status(
                    status_cb,
                    f"Nghỉ {pol.page_delay_min_sec:.0f}–{pol.page_delay_max_sec:.0f}s trước Page tiếp theo…",
                )
                sleep_page_gap(pol)
            _status(status_cb, f"[{i}/{total}] {name}")
            try:
                _assert_page_safe(pg)
                snap = scrape_page_insights_on_tab(
                    pg,
                    page_row=row,
                    period=period,
                    status_cb=status_cb,
                    policy=pol,
                )
            except RuntimeError as exc:
                msg = str(exc)
                if "checkpoint" in msg.lower() or "xác minh" in msg.lower() or "đăng nhập" in msg.lower():
                    batch.stopped_early = True
                    batch.stop_reason = msg
                    if pid:
                        batch.results.append(
                            (
                                pid,
                                {
                                    "period": period,
                                    "followers": None,
                                    "views": None,
                                    "source_url": str(pg.url or ""),
                                    "error": msg,
                                },
                            )
                        )
                    break
                snap = {
                    "period": period,
                    "followers": None,
                    "views": None,
                    "source_url": "",
                    "error": msg,
                }
            except Exception as exc:
                snap = {
                    "period": period,
                    "followers": None,
                    "views": None,
                    "source_url": "",
                    "error": str(exc),
                }
            if pid:
                batch.results.append((pid, snap))
    finally:
        try:
            pg.close()
        except Exception:
            pass
    return batch
