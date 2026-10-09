"""Xem video nguồn theo số giây người dùng đặt, rồi bình luận. Không tải video."""

from __future__ import annotations

import random
from typing import Any, Callable

_PLAY_LABELS = ("play", "phát", "play video", "phát video")
_COMMENT_HINTS = ("comment", "bình luận", "binh luan", "viết bình luận", "write a comment")
_COMMENT_SUBMIT = ("comment", "bình luận", "đăng bình luận", "post comment")


def parse_comment_lines(raw: str) -> list[str]:
    """Mỗi dòng là một bình luận. Bỏ dòng trống và câu trùng."""
    found: list[str] = []
    for line in str(raw or "").replace("\r", "\n").split("\n"):
        text = " ".join(line.split())
        if text and text not in found:
            found.append(text)
    return found


def pick_share_comment(
    lines: list[str],
    used: list[str],
    rng: random.Random | None = None,
) -> str:
    """Lấy ngẫu nhiên một câu chưa dùng. Hết danh sách thì mới chọn lại."""
    pool = [line for line in lines if line not in used]
    if not pool:
        pool = list(lines)
    if not pool:
        return ""
    return (rng or random.Random()).choice(pool)


def clamp_watch_max_minutes(raw: object) -> int:
    """Phút tối đa được xem. Đặt từ 1 đến 30 để xem lâu hơn 1 phút."""
    try:
        minutes = int(str(raw if raw is not None else "").strip())
    except ValueError as exc:
        raise ValueError("Số phút tối đa phải là số từ 1 đến 30") from exc
    if minutes < 1 or minutes > 30:
        raise ValueError("Số phút tối đa phải từ 1 đến 30")
    return minutes


def clamp_watch_seconds(raw: object, max_minutes: int = 1) -> int:
    """Số giây xem video. Từ 5 giây đến số phút tối đa người dùng đặt."""
    limit = clamp_watch_max_minutes(max_minutes) * 60
    try:
        seconds = int(str(raw if raw is not None else "").strip())
    except ValueError as exc:
        raise ValueError(f"Thời gian xem video phải là số giây từ 5 đến {limit}") from exc
    if seconds < 5 or seconds > limit:
        raise ValueError(f"Thời gian xem video phải từ 5 giây đến {limit // 60} phút")
    return seconds


def engage_source_video(
    page: Any,
    *,
    watch_seconds: int,
    comment: str,
    should_stop: Callable[[], bool] | None = None,
) -> dict[str, Any]:
    """Xem đủ số giây, bình luận dưới video, rồi trả về để chia sẻ link.

    Không bấm like, không tải file video. Chưa bình luận xong thì chưa coi là xong.
    """
    url = str(getattr(page, "url", "") or "").casefold()
    if "checkpoint" in url:
        return {"error_code": "CHECKPOINT", "watched_seconds": 0, "commented": False}
    if "/login" in url or "login.php" in url:
        return {"error_code": "SESSION_EXPIRED", "watched_seconds": 0, "commented": False}
    _press_play(page)
    watched_ms = 0
    target_ms = max(0, int(watch_seconds)) * 1000
    while watched_ms < target_ms:
        if should_stop is not None and should_stop():
            return {"error_code": "CANCELLED", "watched_seconds": watched_ms // 1000, "commented": False}
        chunk = min(1000, target_ms - watched_ms)
        page.wait_for_timeout(chunk)
        watched_ms += chunk
    text = str(comment or "").strip()
    if not text:
        return {"error_code": "", "watched_seconds": int(watch_seconds), "commented": False}
    _scroll_to_comments(page)
    _open_comment_composer(page)
    if not _type_comment(page, text):
        return {
            "error_code": "",
            "watched_seconds": int(watch_seconds),
            "commented": False,
            "error_message": "Chưa thấy ô bình luận trên video",
        }
    return {"error_code": "", "watched_seconds": int(watch_seconds), "commented": True}


def _label(item: Any) -> str:
    """Nhãn nút hoặc ô nhập. Ưu tiên aria-label vì nút phát thường không có chữ."""
    for reader in (
        lambda: item.get_attribute("aria-label"),
        lambda: item.inner_text(timeout=400),
    ):
        try:
            text = str(reader() or "").strip().casefold()
        except Exception:  # noqa: BLE001
            text = ""
        if text:
            return text
    return ""


def _press_play(page: Any) -> None:
    """Bấm nút phát nếu video đang dừng. Bỏ qua like và chia sẻ."""
    for item in _visible_role_items(page, "button", limit=20):
        label = _label(item)
        if label not in _PLAY_LABELS:
            continue
        try:
            item.click(timeout=3_000)
            page.wait_for_timeout(400)
        except Exception:  # noqa: BLE001
            return
        return


def _scroll_to_comments(page: Any) -> None:
    """Kéo xuống vùng bình luận sau khi xem xong."""
    evaluate = getattr(page, "evaluate", None)
    if evaluate is not None:
        try:
            evaluate("() => window.scrollBy(0, Math.max(500, (window.innerHeight || 700) * 0.7))")
        except Exception:  # noqa: BLE001
            pass
    try:
        page.wait_for_timeout(400)
    except Exception:  # noqa: BLE001
        return


def _open_comment_composer(page: Any) -> None:
    """Bấm ô «Viết bình luận» nếu ô nhập chưa hiện."""
    if _comment_box(page) is not None:
        return
    for item in _visible_role_items(page, "button", limit=25):
        label = _label(item)
        if not any(hint in label for hint in _COMMENT_HINTS):
            continue
        if label in _PLAY_LABELS or "like" in label or "chia sẻ" in label or label == "share":
            continue
        try:
            item.click(timeout=3_000)
            page.wait_for_timeout(500)
        except Exception:  # noqa: BLE001
            return
        return


def _visible_role_items(page: Any, role: str, *, limit: int) -> list[Any]:
    getter = getattr(page, "get_by_role", None)
    if getter is None:
        return []
    try:
        nodes = getter(role)
        count = min(nodes.count(), limit)
    except Exception:  # noqa: BLE001
        return []
    found: list[Any] = []
    for index in range(count):
        item = nodes.nth(index)
        try:
            if item.is_visible():
                found.append(item)
        except Exception:  # noqa: BLE001
            continue
    return found


def _comment_box(page: Any) -> Any | None:
    for item in _visible_role_items(page, "textbox", limit=12):
        if any(hint in _label(item) for hint in _COMMENT_HINTS):
            return item
    return None


def _type_comment(page: Any, text: str) -> bool:
    """Gõ vào ô bình luận dưới video rồi gửi. Không bấm Đăng bài mới."""
    getter = getattr(page, "get_by_role", None)
    if getter is None:
        return False
    target = _comment_box(page)
    if target is None:
        return False
    try:
        target.click(timeout=4_000)
        target.type(text, delay=40)
        target.press("Enter")
        page.wait_for_timeout(500)
    except Exception:  # noqa: BLE001
        return False
    for item in _visible_role_items(page, "button", limit=25):
        if _label(item) not in _COMMENT_SUBMIT:
            continue
        try:
            item.click(timeout=3_000)
            page.wait_for_timeout(400)
        except Exception:  # noqa: BLE001
            break
        break
    return True
