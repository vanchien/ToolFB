"""Unit-test cho ``schedule_posts_filters`` (search/filter/sort danh sách job)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.utils.schedule_posts_filters import (
    apply_job_filters,
    is_overdue,
    sort_jobs,
)


def _mk(
    *,
    jid: str,
    account: str = "acc_1",
    page: str = "page_A",
    post_type: str = "text",
    status: str = "pending",
    title: str = "",
    image_prompt: str = "",
    ai_language: str = "English",
    scheduled_at: str = "2026-04-22T00:00:00+00:00",
    retry_count: int = 0,
    hashtags: list[str] | None = None,
) -> dict:
    return {
        "id": jid,
        "account_id": account,
        "page_id": page,
        "post_type": post_type,
        "status": status,
        "title": title,
        "image_prompt": image_prompt,
        "ai_language": ai_language,
        "scheduled_at": scheduled_at,
        "retry_count": retry_count,
        "hashtags": list(hashtags or []),
    }


@pytest.fixture()
def sample_jobs() -> list[dict]:
    return [
        _mk(
            jid="j1", account="acc_alpha", page="page_A", post_type="video",
            status="pending", title="Reveal mystery", retry_count=0,
            scheduled_at="2026-04-22T04:30:00+00:00",
        ),
        _mk(
            jid="j2", account="acc_beta", page="page_B", post_type="text_image",
            status="failed", title="Lights in the sky", retry_count=2,
            scheduled_at="2026-04-22T10:15:00+00:00",
            image_prompt="Create one square 1:1 image of lights",
        ),
        _mk(
            jid="j3", account="acc_alpha", page="page_A", post_type="video",
            status="success", title="Hidden footage", retry_count=1,
            scheduled_at="2026-04-21T22:00:00+00:00",
        ),
        _mk(
            jid="j4", account="acc_beta", page="page_B", post_type="text",
            status="pending", title="Bạn có thấy không?", ai_language="Tiếng Việt",
            retry_count=0,
            scheduled_at="2026-04-23T09:00:00+00:00",
            hashtags=["#viral", "#trending"],
        ),
    ]


def test_search_by_keyword_in_title(sample_jobs: list[dict]) -> None:
    out = apply_job_filters(sample_jobs, search_text="reveal")
    assert [j["id"] for j in out] == ["j1"]


def test_search_case_insensitive_and_substring(sample_jobs: list[dict]) -> None:
    out = apply_job_filters(sample_jobs, search_text="LIGHTS")
    assert {j["id"] for j in out} == {"j2"}


def test_search_matches_prompt_and_hashtag(sample_jobs: list[dict]) -> None:
    assert [j["id"] for j in apply_job_filters(sample_jobs, search_text="square 1:1")] == ["j2"]
    assert [j["id"] for j in apply_job_filters(sample_jobs, search_text="#viral")] == ["j4"]


def test_search_matches_scheduled_date(sample_jobs: list[dict]) -> None:
    # 2026-04-23 chỉ có trong j4 (UTC).
    out = apply_job_filters(sample_jobs, search_text="2026-04-23")
    assert [j["id"] for j in out] == ["j4"]


def test_filter_by_account(sample_jobs: list[dict]) -> None:
    out = apply_job_filters(sample_jobs, account="acc_alpha")
    assert {j["id"] for j in out} == {"j1", "j3"}


def test_filter_by_page(sample_jobs: list[dict]) -> None:
    out = apply_job_filters(sample_jobs, page_id="page_B")
    assert {j["id"] for j in out} == {"j2", "j4"}


def test_filter_by_post_type_and_status_combined(sample_jobs: list[dict]) -> None:
    out = apply_job_filters(
        sample_jobs,
        post_type="video",
        status="pending",
    )
    assert [j["id"] for j in out] == ["j1"]


def test_filter_combines_search_and_filters(sample_jobs: list[dict]) -> None:
    out = apply_job_filters(
        sample_jobs,
        search_text="video",  # post_type video sẽ xuất hiện trong haystack
        status="pending",
    )
    assert [j["id"] for j in out] == ["j1"]


def test_retry_mode_retry_0(sample_jobs: list[dict]) -> None:
    out = apply_job_filters(sample_jobs, retry_mode="retry_0")
    assert {j["id"] for j in out} == {"j1", "j4"}


def test_retry_mode_retry_gt_0(sample_jobs: list[dict]) -> None:
    out = apply_job_filters(sample_jobs, retry_mode="retry_gt_0")
    assert {j["id"] for j in out} == {"j2", "j3"}


def test_retry_mode_retry_ge_2(sample_jobs: list[dict]) -> None:
    out = apply_job_filters(sample_jobs, retry_mode="retry_ge_2")
    assert {j["id"] for j in out} == {"j2"}


def test_sort_by_scheduled_at_uses_datetime(sample_jobs: list[dict]) -> None:
    out_asc = sort_jobs(sample_jobs, sort_key="scheduled_at", ascending=True)
    assert [j["id"] for j in out_asc] == ["j3", "j1", "j2", "j4"]
    out_desc = sort_jobs(sample_jobs, sort_key="scheduled_at", ascending=False)
    assert [j["id"] for j in out_desc] == ["j4", "j2", "j1", "j3"]


def test_sort_by_retry_count_uses_int(sample_jobs: list[dict]) -> None:
    out = sort_jobs(sample_jobs, sort_key="retry_count", ascending=False)
    assert [j["id"] for j in out] == ["j2", "j3", "j1", "j4"]


def test_sort_by_title_case_insensitive(sample_jobs: list[dict]) -> None:
    # Order expected: "Bạn có thấy không?", "Hidden footage", "Lights in the sky", "Reveal mystery"
    out = sort_jobs(sample_jobs, sort_key="title", ascending=True)
    assert out[0]["id"] == "j4"
    assert out[-1]["id"] == "j1"


def test_is_overdue_only_pending_past() -> None:
    now = datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc)
    past = (now - timedelta(hours=1)).isoformat()
    future = (now + timedelta(hours=1)).isoformat()
    assert is_overdue(_mk(jid="x", status="pending", scheduled_at=past), now_utc=now) is True
    assert is_overdue(_mk(jid="x", status="pending", scheduled_at=future), now_utc=now) is False
    # Status khác pending thì không coi là overdue.
    assert is_overdue(_mk(jid="x", status="success", scheduled_at=past), now_utc=now) is False
    assert is_overdue(_mk(jid="x", status="failed", scheduled_at=past), now_utc=now) is False


def test_empty_search_returns_all(sample_jobs: list[dict]) -> None:
    assert len(apply_job_filters(sample_jobs, search_text="")) == len(sample_jobs)
    assert len(apply_job_filters(sample_jobs)) == len(sample_jobs)


def test_unknown_sort_key_falls_back_safely(sample_jobs: list[dict]) -> None:
    out = sort_jobs(sample_jobs, sort_key="unknown_column", ascending=True)
    # Không crash — trả về list cùng số phần tử.
    assert len(out) == len(sample_jobs)
