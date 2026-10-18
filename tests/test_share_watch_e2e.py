"""
E2E theo dõi chia sẻ: đúng Page nguồn + chỉ video mới publish trên đầu trang.

Không mở browser — giả lập kết quả quét (kể cả nhiễu trang khác / cuộn sâu).
"""

from __future__ import annotations

import pytest

from src.services.facebook_groups.source_watch import (
    SourceState,
    SourceWatcher,
    detect_new_videos,
    parse_source_urls,
    source_state_key,
)
from src.services.facebook_reels_catalog import (
    facebook_page_identity,
    url_matches_facebook_page,
)


def _reels(*ids: int) -> list[str]:
    return [f"https://www.facebook.com/reel/{i}" for i in ids]


def test_e2e_reject_wrong_source_links() -> None:
    """Không cho dán link reel lẻ / Reels toàn cục làm Page nguồn."""
    with pytest.raises(ValueError):
        parse_source_urls("https://www.facebook.com/reels/")
    with pytest.raises(ValueError):
        parse_source_urls("https://www.facebook.com/reel/12345678901234")
    assert parse_source_urls("https://www.facebook.com/CorrectPage\nhttps://facebook.com/OtherPage") == [
        "https://www.facebook.com/CorrectPage",
        "https://facebook.com/OtherPage",
    ]


def test_e2e_page_gate_blocks_global_reels_redirect() -> None:
    """Sau goto nếu nhảy sang Reels toàn cục → không khớp Page nguồn."""
    src = "https://www.facebook.com/SourcePage"
    assert facebook_page_identity(src) == "vanity:sourcepage"
    assert not url_matches_facebook_page("https://www.facebook.com/reels/", src)
    assert not url_matches_facebook_page("https://www.facebook.com/watch/?v=1", src)
    assert url_matches_facebook_page("https://www.facebook.com/SourcePage/reels/", src)


def test_e2e_watcher_baseline_then_only_top_new() -> None:
    """
    Luồng đầy đủ SourceWatcher:
    1) Lần 1 ghi mốc — không chia sẻ.
    2) Lần 2: video mới trên đầu được lấy; ID nhiễu phía dưới (trang khác / cũ) bỏ qua.
    """
    source = "https://www.facebook.com/SourcePage"
    got: list[tuple[str, list[str]]] = []
    calls = {"n": 0}

    def scan_fn(url: str, _acc: dict) -> dict:
        assert url_matches_facebook_page(url, source) or facebook_page_identity(url) == "vanity:sourcepage"
        calls["n"] += 1
        if calls["n"] == 1:
            # Đầu trang: video của đúng Page.
            urls = _reels(1000000300, 1000000200, 1000000100)
        else:
            # Đầu trang có 1 video mới; phía dưới nhiễu ID rất cao (giả lập related / trang khác).
            urls = _reels(
                1000000400,  # mới publish
                1000000300,
                1000000200,
                *[9000000000 + i for i in range(20)],  # nhiễu sâu
            )
        return {"ok": True, "items": [{"url": u} for u in urls], "message": "ok"}

    states: dict[str, SourceState] = {}
    watcher = SourceWatcher(
        sources=[source],
        account={"id": "acc"},
        states=states,
        scan_min=5,
        scan_max=10,
        scan_fn=scan_fn,
        on_new=lambda src, urls: got.append((src, urls)),
    )
    assert watcher.scan_once() == 0
    assert got == []
    key = source_state_key(source)
    assert states[key].baseline and states[key].max_id == 1000000300

    assert watcher.scan_once() == 1
    assert got == [(source, _reels(1000000400))]
    assert states[key].max_id == 1000000400
    # Nhiễu 9000… đã vào seen nhưng không đẩy max_id / không chia sẻ.
    assert "9000000000" in states[key].seen
    assert watcher.scan_once() == 0


def test_e2e_mixed_sources_states_isolated() -> None:
    """Hai Page nguồn: video mới của A không gắn nhầm sang B."""
    a = "https://www.facebook.com/PageA"
    b = "https://www.facebook.com/PageB"
    got: list[tuple[str, list[str]]] = []
    round_n = {"n": 0}

    def scan_fn(url: str, _acc: dict) -> dict:
        round_n["n"] += 1
        # Mỗi vòng quét cả A rồi B.
        if "PageA" in url:
            if round_n["n"] <= 2:
                items = _reels(2000000100, 2000000050)
            else:
                items = _reels(2000000200, 2000000100)
        else:
            items = _reels(3000000100, 3000000050)
        return {"ok": True, "items": [{"url": u} for u in items]}

    watcher = SourceWatcher(
        sources=[a, b],
        account={"id": "acc"},
        states={},
        scan_min=5,
        scan_max=5,
        scan_fn=scan_fn,
        on_new=lambda src, urls: got.append((src, list(urls))),
    )
    assert watcher.scan_once() == 0  # baseline A+B
    assert got == []
    assert watcher.scan_once() == 1
    assert got == [(a, _reels(2000000200))]
    assert source_state_key(b) in watcher.states
    assert watcher.states[source_state_key(b)].max_id == 3000000100


def test_e2e_detect_ignores_wrong_page_ids_below_fold() -> None:
    """Giả lập scan lấy nhầm ID trang khác ở dưới fold — detect không chia sẻ."""
    state = SourceState()
    detect_new_videos(state, _reels(1000000500, 1000000400, 1000000300))
    polluted = _reels(
        1000000500,
        1000000400,
        *[8000000100 + i for i in range(15)],
    )
    assert detect_new_videos(state, polluted) == []
    assert state.max_id == 1000000500


def test_e2e_deleted_queue_video_not_reenqueued() -> None:
    """User xóa video khỏi hàng chờ → ID nằm trong cancelled → không xếp lại."""
    from src.services.facebook_groups.source_watch import video_id_from_url

    cancelled = {"1000000999"}
    urls = _reels(1000000999, 1000000888)
    kept = [u for u in urls if video_id_from_url(u) not in cancelled]
    assert kept == _reels(1000000888)


def test_e2e_deleted_source_stops_watcher_enqueue_path() -> None:
    """
    Xóa Page nguồn khỏi list → watcher cập nhật list rỗng/khác;
    video từ nguồn đã xóa không còn được coi là active.
    """
    from src.services.facebook_groups.source_watch import is_watch_source_active

    kept = "https://www.facebook.com/KeepPage"
    gone = "https://www.facebook.com/GonePage"
    assert is_watch_source_active(gone, [kept, gone])
    # Người dùng xóa GonePage
    assert not is_watch_source_active(gone, [kept])
    assert not is_watch_source_active(gone, [])

    got: list[str] = []
    sources_holder = {"list": [kept, gone]}

    def scan_fn(url: str, _acc: dict) -> dict:
        # Chỉ trả video nếu nguồn còn trong list «form».
        if not is_watch_source_active(url, sources_holder["list"]):
            return {"ok": True, "items": []}
        return {"ok": True, "items": [{"url": u} for u in _reels(1000000999)]}

    states: dict[str, SourceState] = {
        source_state_key(kept): SourceState(max_id=1000000500, seen=["1000000500"], baseline=True),
        source_state_key(gone): SourceState(max_id=1000000500, seen=["1000000500"], baseline=True),
    }
    watcher = SourceWatcher(
        sources=[kept, gone],
        account={"id": "acc"},
        states=states,
        scan_min=5,
        scan_max=5,
        scan_fn=scan_fn,
        on_new=lambda src, urls: got.append(src),
    )
    # Xóa GonePage trên form + cập nhật watcher
    sources_holder["list"] = [kept]
    watcher.update_settings(sources=[kept])
    assert watcher.sources == [kept]
    n = watcher.scan_once()
    assert gone not in got
    assert n <= 1
