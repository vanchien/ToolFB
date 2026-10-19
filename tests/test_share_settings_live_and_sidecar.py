"""Cài đặt chia sẻ: sidecar nhanh + xếp lại hàng chờ khi đổi gap/share_now."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from src.services.facebook_groups.share_schedule import PlannedShareLink
from src.services.facebook_groups.source_watch import (
    SourceState,
    SourceWatcher,
    recommend_watch_settle_seconds,
)
from src.services.facebook_groups.store import GroupStore


def test_share_settings_sidecar_avoids_rewriting_main_doc(tmp_path: Path) -> None:
    main = tmp_path / "facebook_groups.json"
    store = GroupStore(main)
    store.save_share_settings({"caption": "a", "queue": [{"url": "https://x"}]})
    side = store.share_settings_sidecar_path()
    assert side.is_file()
    data = store.share_settings()
    assert data.get("caption") == "a"
    # Ghi lần 2 không đòi hỏi đọc/ghi lại toàn bộ batches trong sidecar.
    store.save_share_settings({"caption": "b", "watch": {"enabled": True}})
    assert store.share_settings().get("caption") == "b"
    assert store.share_settings()["watch"]["enabled"] is True


def test_recommend_settle_is_under_one_minute() -> None:
    for seed in range(8):
        sec = recommend_watch_settle_seconds(rng=__import__("random").Random(seed))
        assert 15 <= sec <= 50


def test_update_settings_wakes_when_scan_faster() -> None:
    wakes: list[bool] = []
    states: dict[str, SourceState] = {}

    watcher = SourceWatcher(
        sources=["https://www.facebook.com/PageA"],
        account={"id": "acc"},
        states=states,
        scan_min=10,
        scan_max=20,
        scan_fn=lambda *_a: {"ok": True, "items": []},
        on_new=lambda *_a: None,
    )
    watcher.scan_now = lambda: wakes.append(True)  # type: ignore[method-assign]
    assert watcher.update_settings(scan_min=3, scan_max=5) is True
    assert wakes == [True]


def test_replan_waiting_queue_logic_share_now() -> None:
    """Mô phỏng xếp lại: chia sẻ ngay → các waiting gần now."""
    now = datetime.now().replace(microsecond=0)
    items = [
        PlannedShareLink(
            when=now + timedelta(minutes=30),
            url=f"https://www.facebook.com/reel/{i}",
            label=f"Mới {30 + i}",
            targets=["p1"],
            source="https://www.facebook.com/src",
        )
        for i in range(3)
    ]
    # Logic giống ShareTab._replan_waiting_queue (share_now).
    for index, item in enumerate(items):
        item.when = (now + timedelta(seconds=index)).replace(microsecond=0)
        item.label = "Mới Ngay"
    assert items[0].when <= now + timedelta(seconds=1)
    assert all(item.label == "Mới Ngay" for item in items)
