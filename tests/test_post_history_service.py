"""PostHistoryService — rule ảnh / hook / hashtag / fingerprint caption."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.services.post_history_service import PostHistoryService, normalized_hashtag_set
from src.utils import page_workspace as pw


def _now() -> datetime:
    return datetime(2026, 4, 10, 12, 0, 0, tzinfo=timezone.utc)


def test_image_and_hook_cooldown(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(pw, "project_root", lambda: tmp_path)
    page_id = "histpage01"
    pw.ensure_page_workspace(page_id)
    svc = PostHistoryService()
    img = tmp_path / "shot.png"
    img.write_bytes(b"x")
    key = str(img.resolve())
    past = _now() - timedelta(days=5)
    svc.append_entry(
        page_id,
        hook="Khuyến mãi",
        caption="A",
        hashtags=["#a"],
        image_paths=[key],
        posted_at=past,
    )
    assert svc.was_image_used_within_days(page_id, img, days=14, now=_now()) is True
    assert svc.was_image_used_within_days(page_id, img, days=2, now=_now()) is False
    assert svc.was_hook_used_within_days(page_id, "Khuyến mãi", days=7, now=_now()) is True
    assert svc.was_hook_used_within_days(page_id, "Khuyến mãi", days=3, now=_now()) is False


def test_hashtag_streak_blocks_fourth(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(pw, "project_root", lambda: tmp_path)
    page_id = "histpage02"
    pw.ensure_page_workspace(page_id)
    svc = PostHistoryService()
    tags = ["#A", "#b"]
    t0 = _now() - timedelta(hours=3)
    for i in range(3):
        svc.append_entry(
            page_id,
            hook=f"h{i}",
            caption=f"c{i}",
            hashtags=tags,
            posted_at=t0 + timedelta(minutes=i),
        )
    assert svc.same_hashtag_set_streak_from_newest(page_id, normalized_hashtag_set(tags)) == 3
    assert svc.would_block_hashtag_streak(page_id, tags) is True
    assert svc.would_block_hashtag_streak(page_id, ["#x"]) is False


def test_caption_fingerprint(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(pw, "project_root", lambda: tmp_path)
    page_id = "histpage03"
    pw.ensure_page_workspace(page_id)
    svc = PostHistoryService()
    cap = "Dòng mở đầu giống nhau\nphần sau khác"
    svc.append_entry(page_id, hook="x", caption=cap, posted_at=_now() - timedelta(days=1))
    assert svc.caption_fingerprint_used_within_days(page_id, cap, days=7, now=_now()) is True
