"""Kết quả chia sẻ theo từng page."""

from src.gui.share_tab import describe_page_jobs


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
