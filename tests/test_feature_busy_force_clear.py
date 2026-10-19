"""Ép nhả FeatureBusy khi watchdog lịch đăng treo."""

from __future__ import annotations

from src.utils import feature_busy as fb


def setup_function() -> None:
    fb.reset_for_tests()


def teardown_function() -> None:
    fb.reset_for_tests()


def test_force_clear_account_and_matching_page_detail() -> None:
    lease = fb.try_acquire(
        feature=fb.FEATURE_SCHEDULE,
        account_id="acc1",
        page_keys=["page-a", "page-b"],
        detail="sched_hung123",
    )
    assert lease is not None
    assert fb.is_cross_busy(feature=fb.FEATURE_SHARE, account_id="acc1", page_keys=["page-a"])
    n = fb.force_clear_account_feature(
        "acc1", feature=fb.FEATURE_SCHEDULE, detail_substr="sched_hung123"
    )
    assert n >= 1
    assert not fb.is_cross_busy(feature=fb.FEATURE_SHARE, account_id="acc1", page_keys=["page-a"])
