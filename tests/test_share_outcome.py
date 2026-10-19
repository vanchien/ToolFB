"""Kết quả chia sẻ theo từng page."""

from src.gui.share_tab import (
    dedupe_receiving_pages,
    describe_page_jobs,
    receiving_page_key,
    share_has_video_work,
)


def test_same_page_id_across_accounts_keeps_one(monkeypatch) -> None:
    monkeypatch.setattr("src.gui.share_tab.load_page_owner_choices", lambda **_k: {})
    monkeypatch.setattr("src.utils.page_cross_account.load_page_owner_choices", lambda **_k: {})
    rows = [
        {
            "iid": "a1",
            "account_id": "acc1",
            "page_id": "111",
            "page_name": "Happy Stories Time",
            "target_url": "https://www.facebook.com/111",
        },
        {
            "iid": "a2",
            "account_id": "acc2",
            "page_id": "111",
            "page_name": "Happy Stories Time",
            "target_url": "https://www.facebook.com/111",
        },
        {
            "iid": "b1",
            "account_id": "acc1",
            "page_id": "222",
            "page_name": "Other",
            "target_url": "https://www.facebook.com/222",
        },
    ]
    unique = dedupe_receiving_pages(rows)
    assert [row["iid"] for row in unique] == ["a1", "b1"]
    assert receiving_page_key(rows[0]) == receiving_page_key(rows[1])


def test_same_url_without_page_id_keeps_one(monkeypatch) -> None:
    monkeypatch.setattr("src.gui.share_tab.load_page_owner_choices", lambda **_k: {})
    monkeypatch.setattr("src.utils.page_cross_account.load_page_owner_choices", lambda **_k: {})
    rows = [
        {"iid": "x", "page_id": "", "target_url": "https://facebook.com/foo/"},
        {"iid": "y", "page_id": "", "target_url": "https://facebook.com/foo"},
    ]
    assert [row["iid"] for row in dedupe_receiving_pages(rows)] == ["x"]


def test_page_success_and_error_are_named() -> None:
    ok, ok_detail, ok_tag = describe_page_jobs([{"status": "COMPLETED", "error_message": ""}])
    assert ok == "Thành công"
    assert "1" in ok_detail
    assert ok_tag == "ok"
    bad, detail, tag = describe_page_jobs(
        [{"status": "FAILED", "error_message": "Chưa thấy ô đăng"}]
    )
    assert bad == "Lỗi"
    assert detail == "Chưa thấy ô đăng"
    assert tag == "fail"


def test_watch_error_shows_on_pages_still_waiting() -> None:
    label, detail, tag = describe_page_jobs(
        [{"status": "PENDING", "error_message": ""}],
        "Chưa bình luận được dưới video — chưa chia sẻ",
    )
    assert label == "Lỗi"
    assert "bình luận" in detail
    assert tag == "fail"


def test_live_step_shows_which_page_is_running() -> None:
    label, detail, tag = describe_page_jobs(
        [{"status": "PENDING", "error_message": ""}],
        live_step="Đang bấm Like",
    )
    assert label == "Đang chạy"
    assert detail == "Đang bấm Like"
    assert tag == "run"


def test_partial_page_keeps_the_error_text() -> None:
    label, detail, tag = describe_page_jobs(
        [
            {"status": "COMPLETED", "error_message": ""},
            {"status": "FAILED", "error_message": "Nhóm không cho đăng"},
        ]
    )
    assert label == "Một phần"
    assert detail == "Nhóm không cho đăng"
    assert tag == "part"


def test_share_idle_pending_only_is_blank() -> None:
    """Hết video nguồn: không giữ «Đang chờ — Chưa tới lượt» trên Page nhận."""
    label, detail, tag = describe_page_jobs(
        [{"status": "PENDING", "error_message": ""}],
        "",
        "",
        wave_active=True,
        share_idle=True,
    )
    assert label == ""
    assert detail == ""
    assert tag == ""


def test_share_idle_keeps_completed_history() -> None:
    label, detail, tag = describe_page_jobs(
        [
            {"status": "COMPLETED", "error_message": ""},
            {"status": "PENDING", "error_message": ""},
        ],
        share_idle=True,
    )
    assert label == "Thành công"
    assert tag == "ok"


def test_share_has_video_work_from_queue_and_live() -> None:
    assert share_has_video_work() is False
    assert share_has_video_work(queue_states=["done", "skipped"]) is False
    assert share_has_video_work(queue_states=["waiting"]) is True
    assert share_has_video_work(queue_states=["running"]) is True
    assert share_has_video_work(running=True) is False
    assert share_has_video_work(live_batches={"b1"}) is True
