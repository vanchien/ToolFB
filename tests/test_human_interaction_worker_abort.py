"""Worker phân biệt «Dừng» người dùng vs pool join timeout."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.models.mapped_account import MappedAccount
from src.services.human_interaction_worker import run_human_interaction_worker


def test_pool_shutdown_not_reported_as_user_cancel() -> None:
    """``should_abort_worker`` (join) không được ghi «người dùng bấm Dừng»."""
    ma = MappedAccount(account_id="UID_1", use_proxy=False)

    def _pool_abort() -> bool:
        return True

    def _user_cancel() -> bool:
        return False

    with patch(
        "src.services.human_interaction_worker.ensure_mapped_proxy_live",
        return_value=(True, "ok"),
    ), patch(
        "src.services.human_interaction_worker.apply_mapped_secrets_to_vault",
    ):
        result = run_human_interaction_worker(
            ma,
            should_stop=_pool_abort,
            is_user_cancelled=_user_cancel,
        )

    assert result == "interrupted"
    assert ma.status == "pending"
    assert "người dùng" not in str(ma.status_detail or "").lower()
    assert "Dừng" in str(ma.status_detail or "") or "Chạy" in str(ma.status_detail or "")


def test_user_cancel_still_reports_cancelled() -> None:
    ma = MappedAccount(account_id="UID_2", use_proxy=False)

    with patch(
        "src.services.human_interaction_worker.apply_mapped_secrets_to_vault",
    ):
        result = run_human_interaction_worker(
            ma,
            should_stop=lambda: True,
            is_user_cancelled=lambda: True,
        )

    assert result == "cancelled"
    assert ma.status == "cancelled"
    assert "Dừng" in str(ma.status_detail or "")
