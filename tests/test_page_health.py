"""Tình trạng Page cho bảng quản lý nhiều Page / nhiều tài khoản."""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone

from src.services.page_health import (
    HEALTH_FAILING,
    HEALTH_MISSING,
    HEALTH_NO_SCHEDULE,
    HEALTH_OK,
    HEALTH_STALE,
    build_page_health,
    format_last_outcome_cell,
    format_schedule_cell,
    pages_per_account,
    summarize_health,
)

NOW = datetime(2026, 10, 17, 12, 0, tzinfo=timezone.utc)


def _page(pid: str, account: str = "acc1", **extra: object) -> dict:
    """Page đủ link và Meta Page ID."""
    row = {"id": pid, "account_id": account, "page_name": pid, "page_url": f"https://fb.com/{pid}", "fb_page_id": pid}
    row.update(extra)
    return row


def _job(pid: str, status: str, hours: float, **extra: object) -> dict:
    """Job lịch đăng lệch ``hours`` giờ so với NOW."""
    at = (NOW + timedelta(hours=hours)).isoformat()
    row = {"id": f"{pid}-{status}-{hours}", "page_id": pid, "status": status, "scheduled_at": at}
    if status in {"success", "failed"}:
        row["posted_at"] = at
    row.update(extra)
    return row


def test_each_health_rule() -> None:
    """Mỗi quy tắc cho đúng nhãn, theo thứ tự ưu tiên."""
    pages = [
        _page("ok"),
        _page("fail"),
        _page("empty"),
        _page("stale"),
        _page("nolink", page_url=""),
        _page("recovered"),
    ]
    jobs = [
        _job("ok", "success", -2),
        _job("ok", "pending", 3),
        _job("fail", "failed", -3),
        _job("fail", "failed", -1),
        _job("fail", "pending", 2),
        _job("empty", "success", -1),
        _job("stale", "success", -24 * 5),
        _job("stale", "pending", 1),
        _job("nolink", "pending", 1),
        _job("recovered", "failed", -5),
        _job("recovered", "failed", -4),
        _job("recovered", "success", -1),
        _job("recovered", "pending", 4),
    ]
    health = build_page_health(pages, jobs, now=NOW)
    assert health["ok"].health == HEALTH_OK
    assert health["fail"].health == HEALTH_FAILING
    assert health["empty"].health == HEALTH_NO_SCHEDULE
    assert health["stale"].health == HEALTH_STALE
    assert health["nolink"].health == HEALTH_MISSING
    assert health["recovered"].health == HEALTH_OK, "đăng thành công sau lỗi thì hết báo lỗi"


def test_upcoming_count_and_next_time() -> None:
    """Đếm bài chờ (pending/ready_queue/running) và lấy giờ sớm nhất."""
    jobs = [_job("p", "pending", 5), _job("p", "ready_queue", 1), _job("p", "running", 0.5), _job("p", "cancelled", 2)]
    h = build_page_health([_page("p")], jobs, now=NOW)["p"]
    assert h.upcoming == 3
    assert h.next_at == NOW + timedelta(hours=0.5)
    assert format_schedule_cell(h, now=NOW).startswith("3 bài · ")
    assert format_schedule_cell(None) == "—"


def test_page_last_post_field_counts_when_no_job_history() -> None:
    """Page cũ chỉ có ``last_post_at`` trong pages.json vẫn tính lần đăng cuối."""
    page = _page("p", last_post_at="2026-10-17 10:00")
    h = build_page_health([page], [_job("p", "pending", 1)], now=NOW)["p"]
    assert h.last_post_at is not None
    assert h.health == HEALTH_OK


def test_counts_and_per_account() -> None:
    """Tổng hợp theo tình trạng và đếm Page theo tài khoản."""
    pages = [_page("a1", "acc1"), _page("a2", "acc1"), _page("b1", "acc2")]
    health = build_page_health(pages, [_job("a1", "pending", 1)], now=NOW)
    counts = summarize_health(health)
    assert counts[HEALTH_OK] == 1 and counts[HEALTH_NO_SCHEDULE] == 2
    assert summarize_health(health, ["b1"])[HEALTH_NO_SCHEDULE] == 1
    assert pages_per_account(pages) == {"acc1": 2, "acc2": 1}


def test_page_filter_includes_accounts_without_pages() -> None:
    """TK mới trong accounts.json vẫn vào ô lọc Page dù chưa quét Page."""
    from src.services.page_health import account_ids_for_page_filter

    pages = [_page("a1", "acc1"), _page("b1", "acc2")]
    accounts = [{"id": "acc3", "name": "Mới"}, {"id": "acc1", "name": "Cũ"}]
    ids = account_ids_for_page_filter(pages, accounts)
    assert ids == ["acc3", "acc1", "acc2"]


def test_single_recent_fail_is_failing() -> None:
    """Một lần lỗi gần đây (có job chờ) vẫn báo «Lỗi đăng» — dễ lọc trên tab Page."""
    jobs = [
        _job("p", "failed", -2, error_note="Sau 1 lần Next không thấy Post"),
        _job("p", "pending", 3),
    ]
    h = build_page_health([_page("p")], jobs, now=NOW)["p"]
    assert h.health == HEALTH_FAILING
    assert h.last_error.startswith("Sau 1 lần Next")
    assert "Lỗi ·" in format_last_outcome_cell("failed", h)


def test_page_status_failed_without_job_still_failing() -> None:
    """pages.json status=failed mà chưa có job lỗi trong file vẫn hiện cần xử lý."""
    h = build_page_health([_page("p", status="failed")], [], now=NOW)["p"]
    assert h.health == HEALTH_FAILING
    assert format_last_outcome_cell("failed", h) == "Lỗi"


def test_thousands_of_pages_is_fast() -> None:
    """5.000 Page × 50.000 job vẫn tính dưới 2 giây (chạy nền, không treo giao diện)."""
    pages = [_page(f"p{i}", f"acc{i % 60}") for i in range(5000)]
    jobs = [_job(f"p{i % 5000}", ("pending", "success", "failed")[i % 3], (i % 48) - 24) for i in range(50_000)]
    started = time.perf_counter()
    health = build_page_health(pages, jobs, now=NOW)
    assert len(health) == 5000
    assert time.perf_counter() - started < 2.0
