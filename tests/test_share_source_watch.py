"""Theo dõi Page nguồn: chỉ chia sẻ video mới publish, giãn cách ngẫu nhiên giữa các lượt."""

from __future__ import annotations

import random
import time
from datetime import datetime, timedelta

import pytest

from src.services.facebook_groups.share_schedule import (
    PlannedShareLink,
    queue_from_settings,
    queue_to_settings,
)
from src.services.facebook_groups.source_watch import (
    SourceState,
    SourceWatcher,
    detect_new_videos,
    merge_source_states,
    next_share_time,
    parse_minute_range,
    parse_source_urls,
    source_state_key,
    video_id_from_url,
)


def _reels(*ids: int) -> list[str]:
    """Link reel theo thứ tự hiển thị trên tab Reels."""
    return [f"https://www.facebook.com/reel/{i}" for i in ids]


def test_first_scan_only_records_baseline() -> None:
    """Lần đầu: video cũ không được chia sẻ; nhớ video mới nhất trên đầu trang."""
    state = SourceState()
    assert detect_new_videos(state, _reels(1000000300, 1000000200, 1000000100)) == []
    assert state.baseline and state.max_id == 1000000300
    assert state.latest_id == "1000000300"


def test_source_page_display_short_name() -> None:
    from src.services.facebook_groups.source_watch import source_page_display

    assert source_page_display("https://www.facebook.com/healthylifehub2026") == "healthylifehub2026"
    assert source_page_display("https://www.facebook.com/profile.php?id=111490197685020") == (
        "111490197685020"
    )
    assert source_page_display("") == ""
    assert source_page_display(
        "https://www.facebook.com/61594991577702",
        page_name="Healthy Life Hub",
        page_id="61594991577702",
    ) == "Healthy Life Hub (61594991577702)"
    assert source_page_display(
        "https://www.facebook.com/healthylifehub2026",
        page_name="Healthy Life Hub",
        page_id="61594991577702",
    ) == "Healthy Life Hub (61594991577702)"


def test_oldest_first_harvest_feed_is_not_treated_as_new() -> None:
    """GraphQL cũ→mới: video cũ nhất không được chia sẻ như video mới trên đầu trang."""
    newest = [1000000500 - i for i in range(8)]
    state = SourceState()
    detect_new_videos(state, _reels(*newest))
    oldest_first = list(reversed(newest)) + [1000000010, 1000000009]
    assert detect_new_videos(state, _reels(*oldest_first)) == []


def test_only_newer_ids_count_as_new() -> None:
    """Video mới trên đầu trang được lấy, cũ → mới; video cũ lộ ra khi cuộn sâu thì bỏ qua."""
    state = SourceState()
    detect_new_videos(state, _reels(1000000300, 1000000200))
    found = detect_new_videos(state, _reels(1000000500, 1000000400, 1000000300, 1000000200, 1000000050))
    # Giữ thứ tự lưới (mới nhất trước), không đảo theo ID.
    assert found == _reels(1000000500, 1000000400)
    assert detect_new_videos(state, _reels(1000000500, 1000000400)) == [], "không chia sẻ lại"


def test_pinned_known_suggestion_does_not_hide_new_video() -> None:
    """Reel gợi ý đã biết đứng đầu lưới không chặn video mới ngay bên dưới."""
    state = SourceState()
    detect_new_videos(state, _reels(1000000300, 1000000200, 1000000100))
    found = detect_new_videos(
        state,
        _reels(1000000300, 1000000450, 1000000200),
    )
    assert found == _reels(1000000450)


def test_inflated_max_id_does_not_block_new_top_video() -> None:
    """Mốc 17 số (reel gợi ý) không chặn video mới 16 số nằm trên đầu trang."""
    state = SourceState(
        max_id=27741558852187173,
        seen=["2230774414426929", "27741558852187173"],
        baseline=True,
    )
    found = detect_new_videos(
        state,
        _reels(2482358802253174, 2230774414426929),
    )
    assert found == _reels(2482358802253174)


def test_eighteen_digit_junk_ignored() -> None:
    """ID 18 số (nối chuỗi) không vào mốc / list mới."""
    from src.services.facebook_groups.source_watch import video_id_from_url

    assert video_id_from_url("https://www.facebook.com/reel/650496281688565899") == ""
    state = SourceState()
    assert detect_new_videos(state, ["https://www.facebook.com/reel/650496281688565899"]) == []
    assert state.max_id == 0


def test_videos_tab_extra_feed_detects_new() -> None:
    """Video mới chỉ có trên tab Videos vẫn được lấy (feed riêng)."""
    state = SourceState()
    detect_new_videos(state, _reels(1000000300, 1000000200))
    found = detect_new_videos(
        state,
        _reels(1000000300, 1000000200),
        extra_feeds=[_reels(1000000888, 1000000300)],
    )
    assert found == _reels(1000000888)


def test_saved_state_clamps_inflated_max_id() -> None:
    """Đọc settings: max_id 17 số bị kéo về cụm 16 số đã thấy."""
    restored = SourceState.from_dict(
        {
            "max_id": "27741558852187173",
            "seen": ["2230774414426929", "1735485364124493", "27741558852187173"],
            "baseline": True,
            "latest_id": "2230774414426929",
        }
    )
    assert restored.max_id == 2230774414426929
    assert "27741558852187173" in restored.seen
    assert restored.latest_id == "2230774414426929"
    assert restored.to_dict()["latest_id"] == "2230774414426929"


def test_deep_high_id_not_treated_as_new_publish() -> None:
    """ID cao chỉ lộ khi cuộn sâu / nhiễu phía dưới — không coi là vừa publish."""
    from src.services.facebook_groups.source_watch import TOP_NEW_WINDOW

    state = SourceState()
    # Đầu trang cố định 12 ô (giống lưới thật) — không phụ thuộc hằng TOP_NEW_WINDOW.
    head = [1000000300 - i for i in range(12)]
    detect_new_videos(state, _reels(*head))
    assert state.max_id == 1000000300
    # Phía dưới có ID rất cao (video cũ / related) — không vào list mới, không đẩy mốc.
    deep = [*head, 9000000999, 9000000888]
    assert detect_new_videos(state, _reels(*deep)) == []
    assert state.max_id == 1000000300
    assert TOP_NEW_WINDOW >= 12


def test_overflow_new_videos_remain_for_next_scan() -> None:
    """Vượt MAX_NEW_PER_SCAN: chưa xếp thì chưa ghi seen — lần sau lấy tiếp."""
    from src.services.facebook_groups.source_watch import MAX_NEW_PER_SCAN

    state = SourceState()
    detect_new_videos(state, _reels(1000000100, 1000000050))
    batch = [1000000500 - i for i in range(MAX_NEW_PER_SCAN + 3)] + [1000000100]
    first = detect_new_videos(state, _reels(*batch))
    assert len(first) == MAX_NEW_PER_SCAN
    second = detect_new_videos(state, _reels(*batch))
    assert len(second) == 3
    assert detect_new_videos(state, _reels(*batch)) == []


def test_new_publish_at_top_is_detected() -> None:
    """Video mới nằm trên đầu trang (ID lớn hơn mốc) vẫn được nhận."""
    state = SourceState()
    detect_new_videos(state, _reels(1000000300, 1000000200, 1000000100))
    found = detect_new_videos(state, _reels(1000000450, 1000000300, 1000000200))
    assert found == _reels(1000000450)


def test_video_ids_from_various_links() -> None:
    """Nhận ID từ link reel, videos và watch?v=."""
    assert video_id_from_url("https://www.facebook.com/reel/123456789012") == "123456789012"
    assert video_id_from_url("https://www.facebook.com/page/videos/123456789012/") == "123456789012"
    assert video_id_from_url("https://www.facebook.com/watch/?v=123456789012") == "123456789012"
    assert video_id_from_url("https://www.facebook.com/somepage") == ""


def test_parse_sources_and_ranges() -> None:
    """Link nguồn bỏ trùng; khoảng phút nhập ngược được đảo lại; quá nhỏ thì báo lỗi."""
    raw = "https://www.facebook.com/a\nfacebook.com/b, https://www.facebook.com/a/\n"
    assert parse_source_urls(raw) == ["https://www.facebook.com/a", "https://facebook.com/b"]
    with pytest.raises(ValueError):
        parse_source_urls("https://youtube.com/x")
    assert parse_minute_range("90", "30", label="x") == (30, 90)
    with pytest.raises(ValueError):
        parse_minute_range("1", "5", label="Quét", floor=3)


def test_next_share_time_keeps_random_gap_per_page() -> None:
    """Lượt sau cách lượt trước trong khoảng gap; lâu không chia sẻ thì chạy gần như ngay."""
    now = datetime(2026, 10, 17, 22, 0)
    rng = random.Random(3)
    first = next_share_time(now=now, last_planned=None, gap_min=30, gap_max=90, rng=rng)
    assert now <= first <= now + timedelta(minutes=2)
    second = next_share_time(now=now, last_planned=first, gap_min=30, gap_max=90, rng=rng)
    assert timedelta(minutes=30) <= second - first <= timedelta(minutes=90)
    old = now - timedelta(hours=5)
    assert next_share_time(now=now, last_planned=old, gap_min=30, gap_max=90, rng=rng) <= now + timedelta(minutes=2)


def test_watcher_update_settings_applies_live() -> None:
    """Đổi Page nguồn / khoảng quét khi đang chạy không cần tắt theo dõi."""
    watcher = SourceWatcher(
        sources=["https://www.facebook.com/a"],
        account={"id": "acc"},
        states={},
        scan_min=10,
        scan_max=20,
        scan_fn=lambda _url, _acc: {"ok": True, "items": []},
        on_new=lambda _src, _urls: None,
    )
    assert watcher.update_settings(
        sources=["https://www.facebook.com/a", "https://www.facebook.com/b"],
        scan_min=5,
        scan_max=8,
    ) is True
    assert watcher.sources == ["https://www.facebook.com/a", "https://www.facebook.com/b"]
    assert (watcher.scan_min, watcher.scan_max) == (5, 8)
    assert watcher.update_settings(
        sources=["https://www.facebook.com/a", "https://www.facebook.com/b"],
        scan_min=5,
        scan_max=8,
    ) is False


def test_is_watch_source_active_after_delete() -> None:
    """Xóa link Page nguồn → nguồn đó không còn active; nguồn khác vẫn active."""
    from src.services.facebook_groups.source_watch import is_watch_source_active

    active = ["https://www.facebook.com/KeepPage"]
    assert is_watch_source_active("https://www.facebook.com/KeepPage/", active)
    assert not is_watch_source_active("https://www.facebook.com/DeletedPage", active)
    assert not is_watch_source_active("https://www.facebook.com/DeletedPage", [])
    assert not is_watch_source_active("", active)


def test_merge_source_states_keeps_baseline_across_trailing_slash() -> None:
    """Link nguồn thêm/bớt dấu / vẫn giữ mốc — tránh ghi baseline lại khi mở app."""
    primed = SourceState(max_id=1000000500, seen=["1000000500"], baseline=True)
    merged = merge_source_states(
        {"https://www.facebook.com/Src/": primed},
        ["https://www.facebook.com/src"],
    )
    key = source_state_key("https://www.facebook.com/src")
    assert merged[key].baseline is True
    assert merged[key].max_id == 1000000500


def test_watcher_reuses_baseline_when_source_url_slash_differs() -> None:
    """Watcher không coi lần quét sau restart là lần đầu chỉ vì URL khác dấu /."""
    states = {
        "https://www.facebook.com/src/": SourceState(
            max_id=1000000200, seen=["1000000200", "1000000100"], baseline=True
        )
    }
    got: list[tuple[str, list[str]]] = []
    watcher = SourceWatcher(
        sources=["https://www.facebook.com/src"],
        account={"id": "acc"},
        states=states,
        scan_min=5,
        scan_max=10,
        scan_fn=lambda _url, _acc: {
            "ok": True,
            "items": [{"url": u} for u in _reels(1000000300, 1000000200)],
        },
        on_new=lambda src, urls: got.append((src, urls)),
    )
    assert watcher.scan_once() == 1
    assert got == [("https://www.facebook.com/src", _reels(1000000300))]
    assert states[source_state_key("https://www.facebook.com/src")].max_id == 1000000300


def test_watcher_scan_once_reports_new_videos_and_keeps_state() -> None:
    """Vòng 1 ghi mốc, vòng 2 báo video mới; lỗi quét không làm mất mốc."""
    pages = {"https://www.facebook.com/src": [_reels(1000000200, 1000000100)]}
    responses = [
        {"ok": True, "items": [{"url": u} for u in _reels(1000000200, 1000000100)]},
        {"ok": False, "items": [], "message": "checkpoint"},
        {"ok": True, "items": [{"url": u} for u in _reels(1000000300, 1000000200)]},
    ]
    got: list[tuple[str, list[str]]] = []
    states: dict[str, SourceState] = {}
    watcher = SourceWatcher(
        sources=list(pages),
        account={"id": "acc"},
        states=states,
        scan_min=5,
        scan_max=10,
        scan_fn=lambda _url, _acc: responses.pop(0),
        on_new=lambda src, urls: got.append((src, urls)),
    )
    assert watcher.scan_once() == 0
    assert watcher.scan_once() == 0
    assert "Lỗi quét" in states["https://www.facebook.com/src"].note
    assert watcher.scan_once() == 1
    assert got == [("https://www.facebook.com/src", _reels(1000000300))]
    restored = SourceState.from_dict(states["https://www.facebook.com/src"].to_dict())
    assert restored.baseline and restored.max_id == 1000000300


def test_start_without_immediate_scan_waits_until_wake() -> None:
    """Mở app: không quét ngay — chờ scan_now / hết khoảng nghỉ, tránh đụng profile lịch đăng."""
    calls: list[str] = []
    watcher = SourceWatcher(
        sources=["https://www.facebook.com/a"],
        account={"id": "acc"},
        states={},
        scan_min=5,
        scan_max=5,
        scan_fn=lambda url, _acc: calls.append(url) or {"ok": True, "items": []},
        on_new=lambda _src, _urls: None,
    )
    watcher.start(scan_immediately=False)
    time.sleep(0.25)
    assert calls == []
    watcher.scan_now()
    deadline = time.time() + 2.0
    while not calls and time.time() < deadline:
        time.sleep(0.05)
    watcher.stop()
    assert calls == ["https://www.facebook.com/a"]


def test_queue_keeps_targets_and_source_after_restart() -> None:
    """Link do theo dõi thêm vẫn nhớ Page nhận và Page nguồn khi mở lại app."""
    item = PlannedShareLink(
        when=datetime(2026, 10, 17, 23, 0),
        url="https://www.facebook.com/reel/1000000300",
        label="Mới 23:00",
        targets=["p1", "p2"],
        source="https://www.facebook.com/src",
    )
    back = queue_from_settings(queue_to_settings([item]))
    assert back[0].targets == ["p1", "p2"]
    assert back[0].source == "https://www.facebook.com/src"
