"""Mở lại theo dõi: chờ settle ngắn, không chờ cả khoảng quét."""

from __future__ import annotations

from src.services.facebook_groups.source_watch import SourceState, SourceWatcher


def test_watch_start_settle_caps_under_two_minutes(tmp_path) -> None:
    waits: list[float] = []
    states: dict[str, SourceState] = {}

    def scan_fn(url: str, _acc: dict) -> dict:
        del url
        return {"ok": True, "items": [], "message": "ok"}

    watcher = SourceWatcher(
        sources=["https://www.facebook.com/PageA"],
        account={"id": "acc"},
        states=states,
        scan_min=10,
        scan_max=20,
        scan_fn=scan_fn,
        on_new=lambda *_a: None,
        rng=__import__("random").Random(1),
    )
    # Ghi đè wait để không ngủ thật.
    original_wait = watcher._wake.wait

    def _fake_wait(timeout: float | None = None) -> bool:
        waits.append(float(timeout or 0))
        watcher._stop.set()
        return True

    watcher._wake.wait = _fake_wait  # type: ignore[method-assign]
    watcher.start(scan_immediately=False)
    watcher._thread.join(timeout=2.0)  # type: ignore[union-attr]
    watcher._wake.wait = original_wait  # type: ignore[method-assign]
    assert waits, "Phải chờ settle trước lần quét đầu"
    # Settle thông minh: lịch trống ~18–32s; lịch bận ~28–45s (không còn 45–90).
    assert 15 <= waits[0] <= 50
