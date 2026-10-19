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


def test_e2e_empty_first_scan_does_not_baseline() -> None:
    """Quét rỗng lần đầu không ghi baseline — tránh lần sau lấy nhầm hết video cũ."""
    source = "https://www.facebook.com/EmptyThenReady"
    calls = {"n": 0}
    got: list[list[str]] = []

    def scan_fn(_url: str, _acc: dict) -> dict:
        calls["n"] += 1
        if calls["n"] == 1:
            return {"ok": True, "items": []}
        return {"ok": True, "items": [{"url": u} for u in _reels(1000000300, 1000000200)]}

    watcher = SourceWatcher(
        sources=[source],
        account={"id": "acc"},
        states={},
        scan_min=5,
        scan_max=5,
        scan_fn=scan_fn,
        on_new=lambda _s, urls: got.append(list(urls)),
    )
    assert watcher.scan_once() == 0
    st = watcher.states[source_state_key(source)]
    assert not st.baseline
    assert st.seen == []
    assert watcher.scan_once() == 0  # lần này mới ghi mốc
    assert got == []
    assert st.baseline and st.latest_id == "1000000300"
    assert watcher.scan_once() == 0


def test_e2e_many_new_videos_not_lost_across_scans() -> None:
    """Nhiều video mới hơn MAX_NEW_PER_SCAN: lần sau vẫn lấy phần còn lại (không bỏ sót)."""
    from src.services.facebook_groups.source_watch import MAX_NEW_PER_SCAN

    source = "https://www.facebook.com/BusyPage"
    # 3 cũ + 10 mới trên đầu
    old = [1000000300, 1000000200, 1000000100]
    new_ids = [1000001000 + i for i in range(10)]  # 1000..1009 mới nhất trước
    top = list(reversed(new_ids)) + old  # 1009, 1008, … 1000, rồi cũ
    got: list[str] = []

    def scan_fn(_url: str, _acc: dict) -> dict:
        return {"ok": True, "items": [{"url": u} for u in _reels(*top)]}

    watcher = SourceWatcher(
        sources=[source],
        account={"id": "acc"},
        states={},
        scan_min=5,
        scan_max=5,
        scan_fn=scan_fn,
        on_new=lambda _s, urls: got.extend(urls),
    )
    # Baseline với chỉ 3 video cũ
    detect_new_videos(watcher.states.setdefault(source_state_key(source), SourceState()), _reels(*old))
    assert watcher.scan_once() == MAX_NEW_PER_SCAN
    assert len(got) == MAX_NEW_PER_SCAN
    # Phần còn lại vẫn được lấy ở vòng sau
    n2 = watcher.scan_once()
    assert n2 == 10 - MAX_NEW_PER_SCAN
    assert len(got) == 10
    assert set(got) == set(_reels(*reversed(new_ids)))
    assert watcher.scan_once() == 0


def test_e2e_sequential_list_remembers_each_page_history() -> None:
    """Quét lần lượt theo list: mỗi Page nhớ lịch sử riêng, video mới đúng Page."""
    pages = [
        "https://www.facebook.com/Healthylifehub2026",
        "https://www.facebook.com/NiceGiftShops",
        "https://www.facebook.com/Arad.ASMR",
    ]
    order: list[str] = []
    feeds = {
        pages[0]: _reels(1100000300, 1100000200),
        pages[1]: _reels(2200000300, 2200000200),
        pages[2]: _reels(3300000300, 3300000200),
    }
    round_n = {"n": 0}
    got: list[tuple[str, list[str]]] = []

    def scan_fn(url: str, _acc: dict) -> dict:
        order.append(url)
        round_n["n"] += 1
        items = list(feeds[url])
        # Vòng 2: chỉ Page giữa có video mới trên đầu
        if round_n["n"] > 3 and url == pages[1]:
            items = _reels(2200000400, 2200000300, 2200000200)
        return {"ok": True, "items": [{"url": u} for u in items], "page_name": url.rsplit("/", 1)[-1]}

    watcher = SourceWatcher(
        sources=pages,
        account={"id": "acc"},
        states={},
        scan_min=5,
        scan_max=5,
        scan_fn=scan_fn,
        on_new=lambda src, urls: got.append((src, list(urls))),
    )
    assert watcher.scan_once() == 0
    assert order == pages  # đúng thứ tự list
    for p in pages:
        st = watcher.states[source_state_key(p)]
        assert st.baseline
        assert st.latest_id.endswith("300")
        assert len(st.seen) >= 2

    order.clear()
    assert watcher.scan_once() == 1
    assert order == pages
    assert got == [(pages[1], _reels(2200000400))]
    # Page khác không bị nhảy mốc / không nhận nhầm video
    assert watcher.states[source_state_key(pages[0])].latest_id == "1100000300"
    assert watcher.states[source_state_key(pages[2])].latest_id == "3300000300"


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
