from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.utils.page_insights_policy import PageInsightsPolicy, plan_page_insights_fetch
from src.utils.page_insights_store import PageInsightsStore


def test_should_skip_fetch_when_fresh(tmp_path) -> None:
    store = PageInsightsStore(tmp_path / "pi.json")
    store.save_snapshot("p1", "7d", followers=100, views=200)
    skip, reason = store.should_skip_fetch("p1", "7d", min_hours_success=12, min_hours_error=2)
    assert skip is True
    assert "mới" in reason


def test_plan_respects_max_per_run(tmp_path) -> None:
    store = PageInsightsStore(tmp_path / "pi.json")
    pages = [{"id": f"p{i}", "account_id": "a1", "page_name": f"P{i}"} for i in range(5)]
    pol = PageInsightsPolicy(
        min_interval_hours=12,
        min_retry_error_hours=2,
        max_pages_per_run=2,
        account_cooldown_min=0,
        page_delay_min_sec=1,
        page_delay_max_sec=2,
        url_delay_min_sec=1,
        url_delay_max_sec=2,
        force_refresh=True,
    )
    plan = plan_page_insights_fetch(pages, period="7d", store=store, policy=pol, force_refresh=True)
    assert len(plan.to_fetch) == 2
    assert len(plan.deferred_over_limit) == 3


def test_account_cooldown(tmp_path) -> None:
    store = PageInsightsStore(tmp_path / "pi.json")
    store.touch_account_session("acc_a")
    ok, msg = store.account_can_start_session("acc_a", cooldown_min=60)
    assert ok is False
    assert "phút" in msg
