"""Lướt Reel, xem video nguồn theo khoảng giây người dùng đặt, vừa xem vừa Like/bình luận. Không tải video."""

from __future__ import annotations

import json
import random
import time
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Callable

# Tỷ lệ tối thiểu mặc định (%) video nguồn trong ngày có Like hoặc bình luận.
DEFAULT_ENGAGE_RATE_PERCENT = 65
# Ít khi vừa Like vừa bình luận cùng một video (tránh mọi video đều cả hai).
SHARE_ENGAGE_BOTH_RATE = 0.06
# Giữ tương thích test / import cũ.
MIN_SHARE_ENGAGE_RATE = DEFAULT_ENGAGE_RATE_PERCENT / 100.0
SHARE_ENGAGE_ROLL = 0.78


def clamp_engage_rate_percent(raw: object, *, default: int = DEFAULT_ENGAGE_RATE_PERCENT) -> int:
    """
    Phần trăm tối thiểu video trong ngày có Like hoặc bình luận (0–100).

    Người dùng nhập trên form; trống / sai → mặc định.
    """
    text = str(raw if raw is not None else "").strip().replace("%", "")
    if not text:
        return int(default)
    try:
        value = int(float(text.replace(",", ".")))
    except ValueError as exc:
        raise ValueError("Tỷ lệ Like/comment phải là số từ 0 đến 100") from exc
    if value < 0 or value > 100:
        raise ValueError("Tỷ lệ Like/comment phải từ 0 đến 100")
    return value


def _engage_roll_for_rate(min_rate: float) -> float:
    """Xác suất vẫn tương tác khi đã đủ ngưỡng — hơi cao hơn min_rate."""
    return min(0.95, max(0.05, float(min_rate) + 0.12))

_PLAY_LABELS = ("play", "phát", "play video", "phát video")
_COMMENT_HINTS = ("comment", "bình luận", "binh luan", "viết bình luận", "write a comment")
_COMMENT_ICON_JS = """() => {
  const mark = "M12 .5C18.351";
  for (const path of document.querySelectorAll("svg path")) {
    const shape = path.getAttribute("d") || "";
    if (!shape.startsWith(mark)) continue;
    const svg = path.closest("svg");
    let node = svg;
    let sized = svg && svg.parentElement;
    for (let depth = 0; depth < 8 && node; depth += 1) {
      node = node.parentElement;
      if (!node) break;
      const role = (node.getAttribute("role") || "").toLowerCase();
      const label = (node.getAttribute("aria-label") || "").toLowerCase();
      if (role === "button" || node.tagName === "BUTTON") return node;
      if (label.startsWith("comment") || label.startsWith("bình luận") || label.startsWith("binh luan")) return node;
      const rect = node.getBoundingClientRect();
      if (rect.width >= 36 && rect.width <= 180 && rect.height >= 36 && rect.height <= 240) sized = node;
    }
    return sized;
  }
  return null;
}"""
_COMMENT_SUBMIT = ("comment", "bình luận", "đăng bình luận", "post comment")
_VIDEO_SHARE = ("share", "chia sẻ", "chia se")
_SHARE_SKIP = ("story", "tin", "messenger", "whatsapp", "copy", "sao chép", "like", "thích", "comment", "bình luận")
_LIKE_WORDS = ("like", "thích", "thich")
_LIKE_DONE = ("unlike", "bỏ thích", "bo thich", "remove like", "đã thích", "da thich", "liked")
_PAGE_SHARE = ("page", "trang", "feed", "bảng tin", "share now", "chia sẻ ngay")
_GROUP_SHARE = ("group", "nhóm", "nhom")
_SHARE_CONFIRM = ("post", "đăng", "đăng bài")
_SHARE_CONFIRM_ALT = ("share now", "chia sẻ ngay")
_POST_AS = ("post as", "posting as", "đăng với tư cách", "đăng dưới tên")


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


@dataclass(frozen=True)
class ShareEngagePlan:
    """Kế hoạch Like / bình luận cho một video nguồn trước khi chia sẻ."""

    do_like: bool
    do_comment: bool
    mode: str  # none | like | comment | both

    def to_dict(self) -> dict[str, Any]:
        """Dạng lưu lịch sử trong ngày."""
        return {
            "do_like": bool(self.do_like),
            "do_comment": bool(self.do_comment),
            "mode": str(self.mode),
        }


def _engage_mode(do_like: bool, do_comment: bool) -> str:
    if do_like and do_comment:
        return "both"
    if do_like:
        return "like"
    if do_comment:
        return "comment"
    return "none"


def share_engage_source_key(source: str, video_url: str = "") -> str:
    """Khóa Page nguồn để cân bằng Like/comment trong ngày."""
    raw = str(source or "").strip()
    if raw:
        try:
            from src.services.facebook_groups.source_watch import source_state_key

            return source_state_key(raw)
        except Exception:
            return raw.rstrip("/").casefold()
    vid = ""
    try:
        from src.services.facebook_groups.source_watch import video_id_from_url

        vid = video_id_from_url(video_url)
    except Exception:
        vid = ""
    return f"video:{vid}" if vid else "manual"


def pick_share_engage_plan(
    *,
    source: str = "",
    video_url: str = "",
    video_id: str = "",
    allow_like: bool = True,
    allow_comment: bool = True,
    history: list[dict[str, Any]] | None = None,
    rng: random.Random | None = None,
    min_engage_rate: float | None = None,
) -> ShareEngagePlan:
    """
    Chọn Like / bình luận cho một video nguồn.

    - ≥ ``min_engage_rate`` (0–1, mặc định 65%) video trong ngày có ít nhất một tương tác.
    - Các video đã tương tác chia ~50/50 Like-only vs comment-only.
    - Ít khi vừa Like vừa bình luận cùng một video.
    """
    pick = rng or random.Random()
    floor = (
        MIN_SHARE_ENGAGE_RATE
        if min_engage_rate is None
        else max(0.0, min(1.0, float(min_engage_rate)))
    )
    roll = _engage_roll_for_rate(floor)
    vid = str(video_id or "").strip()
    if not vid and video_url:
        try:
            from src.services.facebook_groups.source_watch import video_id_from_url

            vid = video_id_from_url(video_url)
        except Exception:
            vid = ""
    prior = [dict(row) for row in (history or []) if isinstance(row, dict)]
    if vid:
        for row in prior:
            if str(row.get("video_id") or "") == vid:
                do_like = bool(row.get("do_like")) and allow_like
                do_comment = bool(row.get("do_comment")) and allow_comment
                return ShareEngagePlan(do_like, do_comment, _engage_mode(do_like, do_comment))

    can_like = bool(allow_like)
    can_comment = bool(allow_comment)
    if not can_like and not can_comment:
        return ShareEngagePlan(False, False, "none")

    n_after = len(prior) + 1
    engaged = sum(1 for row in prior if row.get("do_like") or row.get("do_comment"))
    # Bắt buộc tương tác nếu bỏ qua sẽ kéo tỷ lệ ngày xuống dưới ngưỡng người dùng đặt.
    must_engage = engaged / n_after < floor
    if not must_engage and pick.random() >= roll:
        return ShareEngagePlan(False, False, "none")

    like_only = sum(1 for row in prior if row.get("do_like") and not row.get("do_comment"))
    comment_only = sum(1 for row in prior if row.get("do_comment") and not row.get("do_like"))

    if can_like and can_comment:
        if pick.random() < SHARE_ENGAGE_BOTH_RATE:
            return ShareEngagePlan(True, True, "both")
        if like_only < comment_only:
            return ShareEngagePlan(True, False, "like")
        if comment_only < like_only:
            return ShareEngagePlan(False, True, "comment")
        if pick.random() < 0.5:
            return ShareEngagePlan(True, False, "like")
        return ShareEngagePlan(False, True, "comment")
    if can_like:
        return ShareEngagePlan(True, False, "like")
    return ShareEngagePlan(False, True, "comment")


def append_share_engage_history(
    history: list[dict[str, Any]],
    *,
    video_id: str,
    plan: ShareEngagePlan,
    max_items: int = 80,
) -> list[dict[str, Any]]:
    """Ghi kế hoạch video vào lịch sử ngày (bỏ trùng theo video_id)."""
    vid = str(video_id or "").strip()
    out = [dict(row) for row in history if isinstance(row, dict) and str(row.get("video_id") or "") != vid]
    row = plan.to_dict()
    row["video_id"] = vid
    out.append(row)
    return out[-max(1, int(max_items)) :]


def default_share_engage_store_path() -> Path:
    """File lưu phân bổ Like/comment theo ngày + Page nguồn."""
    root = Path(__file__).resolve().parents[3]
    return root / "data" / "share_engage_day.json"


def load_share_engage_day(
    path: Path | None = None,
    *,
    day: str | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Đọc lịch sử ngày hiện tại: ``{ source_key: [ {video_id, do_like, do_comment, mode} ] }``."""
    store = path or default_share_engage_store_path()
    today = day or date.today().isoformat()
    try:
        raw = json.loads(store.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(raw, dict):
        return {}
    block = raw.get(today)
    if not isinstance(block, dict):
        return {}
    out: dict[str, list[dict[str, Any]]] = {}
    for key, rows in block.items():
        if isinstance(rows, list):
            out[str(key)] = [dict(r) for r in rows if isinstance(r, dict)]
    return out


def save_share_engage_day(
    by_source: dict[str, list[dict[str, Any]]],
    path: Path | None = None,
    *,
    day: str | None = None,
    keep_days: int = 5,
) -> None:
    """Ghi lịch sử ngày; xóa ngày cũ hơn ``keep_days``."""
    store = path or default_share_engage_store_path()
    today = day or date.today().isoformat()
    doc: dict[str, Any] = {}
    try:
        loaded = json.loads(store.read_text(encoding="utf-8"))
        if isinstance(loaded, dict):
            doc = loaded
    except Exception:
        doc = {}
    doc[today] = {str(k): list(v or []) for k, v in (by_source or {}).items()}
    cutoff = (date.today() - timedelta(days=max(1, int(keep_days)))).isoformat()
    pruned = {k: v for k, v in doc.items() if str(k) >= cutoff and isinstance(v, dict)}
    store.parent.mkdir(parents=True, exist_ok=True)
    store.write_text(json.dumps(pruned, ensure_ascii=False, indent=2), encoding="utf-8")


def remember_share_engage_plan(
    *,
    source: str,
    video_url: str,
    plan: ShareEngagePlan,
    path: Path | None = None,
) -> ShareEngagePlan:
    """Lưu kế hoạch vào file ngày — lần chia sẻ lại cùng video giữ nguyên Like/comment."""
    from src.services.facebook_groups.source_watch import video_id_from_url

    vid = video_id_from_url(video_url)
    key = share_engage_source_key(source, video_url)
    day_map = load_share_engage_day(path)
    hist = list(day_map.get(key) or [])
    day_map[key] = append_share_engage_history(hist, video_id=vid, plan=plan)
    save_share_engage_day(day_map, path)
    return plan


def plan_share_engage_for_video(
    *,
    source: str = "",
    video_url: str = "",
    allow_like: bool = True,
    allow_comment: bool = True,
    rng: random.Random | None = None,
    path: Path | None = None,
    persist: bool = True,
    min_engage_rate: float | None = None,
    engage_rate_percent: object = None,
) -> ShareEngagePlan:
    """Bốc kế hoạch Like/comment cho video (cân bằng theo Page nguồn trong ngày) rồi lưu."""
    from src.services.facebook_groups.source_watch import video_id_from_url

    floor = min_engage_rate
    if floor is None and engage_rate_percent is not None:
        floor = clamp_engage_rate_percent(engage_rate_percent) / 100.0
    key = share_engage_source_key(source, video_url)
    vid = video_id_from_url(video_url)
    day_map = load_share_engage_day(path)
    hist = list(day_map.get(key) or [])
    plan = pick_share_engage_plan(
        source=source,
        video_url=video_url,
        video_id=vid,
        allow_like=allow_like,
        allow_comment=allow_comment,
        history=hist,
        rng=rng,
        min_engage_rate=floor,
    )
    if persist:
        day_map[key] = append_share_engage_history(hist, video_id=vid, plan=plan)
        save_share_engage_day(day_map, path)
    return plan


def allocate_wave_page_engage(
    page_ids: list[str],
    *,
    want_like: bool = False,
    want_comment: bool = False,
    rng: random.Random | None = None,
) -> tuple[set[str], set[str]]:
    """
    Phân bổ Like / comment thưa trên các Page nhận của một lượt chia sẻ.

    Ví dụ 13 Page chia sẻ: khoảng 2–3 Like và 3–4 bình luận, Page khác chỉ chia sẻ.
    Like và comment không trùng cùng một Page (trừ khi chỉ còn 1 Page).
    """
    pick = rng or random.Random()
    ids = list(dict.fromkeys(str(p).strip() for p in page_ids if str(p).strip()))
    n = len(ids)
    if n <= 0:
        return set(), set()
    like_n = 0
    comment_n = 0
    if want_like:
        # ~15–25% — 13 Page → 2–3
        lo = max(1, int(round(n * 0.15))) if n >= 2 else 1
        hi = max(lo, int(round(n * 0.25)))
        like_n = pick.randint(lo, min(hi, n))
    if want_comment:
        # ~23–31% — 13 Page → 3–4
        lo = max(1, int(round(n * 0.23))) if n >= 2 else 1
        hi = max(lo, int(round(n * 0.31)))
        comment_n = pick.randint(lo, min(hi, n))
    if like_n + comment_n > n:
        overflow = like_n + comment_n - n
        # Ưu tiên giữ comment (user muốn comment hơi nhiều hơn like).
        cut_like = min(like_n, (overflow + 1) // 2)
        like_n -= cut_like
        overflow -= cut_like
        comment_n = max(0, comment_n - overflow)
    shuffled = list(ids)
    pick.shuffle(shuffled)
    like_set = set(shuffled[:like_n])
    comment_set = set(shuffled[like_n : like_n + comment_n])
    return like_set, comment_set


def shuffle_due_queue_items(items: list[Any], *, now: Any = None, rng: random.Random | None = None) -> Any | None:
    """
    Chọn ngẫu nhiên một video đã tới giờ trong hàng chờ (không luôn lấy dòng trên cùng).

    ``items``: list ``PlannedShareLink`` (có ``.when``, ``.state``).
    Giờ được chuẩn hóa naive local — tránh TypeError naive/aware làm worker dừng.
    """
    from datetime import datetime as _dt

    from src.services.facebook_groups.share_schedule import as_local_naive

    moment = as_local_naive(now or _dt.now())
    pick = rng or random.Random()
    due: list[Any] = []
    waiting: list[Any] = []
    for row in items:
        if getattr(row, "state", "") != "waiting":
            continue
        waiting.append(row)
        try:
            when = as_local_naive(getattr(row, "when", moment))
        except Exception:  # noqa: BLE001
            when = moment
        # Ghi lại when đã chuẩn hóa — lần sau so sánh không lỗi.
        try:
            row.when = when
        except Exception:  # noqa: BLE001
            pass
        if when <= moment:
            due.append(row)
    if due:
        return pick.choice(due)
    if not waiting:
        return None
    return min(waiting, key=lambda row: as_local_naive(getattr(row, "when", moment)))


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


def clamp_reel_seconds(raw: object) -> int:
    """Số giây lướt Reel trước khi mở video. 0 là bỏ qua, tối đa 3 phút."""
    try:
        seconds = int(str(raw if raw is not None else "").strip() or "0")
    except ValueError as exc:
        raise ValueError("Thời gian lướt Reel phải là số giây từ 0 đến 180") from exc
    if seconds < 0 or seconds > 180:
        raise ValueError("Thời gian lướt Reel phải từ 0 đến 180 giây")
    return seconds


def optional_watch_bounds(low_raw: object, high_raw: object) -> tuple[int, int]:
    """Để trống cả hai ô thì không xem video. Chỉ điền một ô thì dùng đúng số đó."""
    low_text = str(low_raw if low_raw is not None else "").strip()
    high_text = str(high_raw if high_raw is not None else "").strip()
    if not low_text and not high_text:
        return 0, 0
    if not low_text:
        low_text = high_text
    if not high_text:
        high_text = low_text
    return clamp_watch_bounds(low_text, high_text)


def clamp_watch_bounds(low_raw: object, high_raw: object) -> tuple[int, int]:
    """Khoảng giây xem video nguồn. Random trong khoảng người dùng đặt, từ 1 giây đến 1 phút."""
    try:
        low = int(str(low_raw if low_raw is not None else "").strip())
        high = int(str(high_raw if high_raw is not None else "").strip())
    except ValueError as exc:
        raise ValueError("Thời gian xem video phải là số giây từ 1 đến 60") from exc
    if low < 1 or high < 1 or low > 60 or high > 60:
        raise ValueError("Thời gian xem video phải từ 1 giây đến 1 phút")
    if high < low:
        raise ValueError("Số giây xem đến phải lớn hơn hoặc bằng số giây bắt đầu")
    return low, high


def pick_page_watch_seconds(low: int, high: int, rng: random.Random | None = None) -> int:
    """Một page một thời lượng. Cùng khoảng người dùng đặt, page khác thì số giây khác."""
    start = max(0, int(low))
    end = max(start, int(high))
    if end <= 0:
        return 0
    return (rng or random.Random()).randint(start, end)


_REELS_HOME = "https://www.facebook.com/reels/"
_REEL_BROKEN_MARKERS = (
    "this page isn't available",
    "this page isn’t available",
    "this content isn't available",
    "this content isn’t available",
    "content isn't available right now",
    "this video isn't available",
    "this video is unavailable",
    "reel isn't available",
    "try reloading this page",
    "we're working to get fixed",
    "the link you followed may be broken",
    "nội dung này hiện không có",
    "nội dung này hiện không khả dụng",
    "trang này hiện không khả dụng",
    "trang này không khả dụng",
    "trang này không hiển thị",
    "thử tải lại trang",
    "video này không khả dụng",
)


def _page_visible_text(page: Any) -> str:
    """Chữ trên màn hình (không đọc mã nguồn)."""
    reader = getattr(page, "inner_text", None)
    if callable(reader):
        try:
            return str(reader("body") or "")
        except TypeError:
            try:
                return str(reader() or "")
            except Exception:  # noqa: BLE001
                pass
        except Exception:  # noqa: BLE001
            pass
    extra = getattr(page, "visible_text", None)
    if extra is not None and not callable(extra):
        return str(extra or "")
    return ""


def reel_page_is_broken(page: Any) -> bool:
    """Reel/video Facebook báo lỗi kỹ thuật / không còn xem được."""
    text = " ".join(_page_visible_text(page).split()).casefold()
    if not text:
        return False
    return any(marker in text for marker in _REEL_BROKEN_MARKERS)


def _reel_identity(page: Any) -> str:
    """ID reel đang mở, hoặc URL nếu không tách được."""
    url = str(getattr(page, "url", "") or "")
    try:
        from src.services.facebook_groups.source_watch import video_id_from_url

        vid = video_id_from_url(url)
        if vid:
            return vid
    except Exception:  # noqa: BLE001
        pass
    return url.rstrip("/").casefold()


def skip_broken_reel(page: Any, *, attempts: int = 5) -> bool:
    """
    Reel lỗi → ArrowDown hoặc mở ``/reels/`` lấy clip khác.

    True nếu đã rời trang lỗi.
    """
    if not reel_page_is_broken(page):
        return True
    origin = _reel_identity(page)
    for index in range(max(1, int(attempts))):
        if index == 1:
            _goto_reels_home(page)
        else:
            _next_reel(page)
        _pause(page, 700)
        if reel_page_is_broken(page):
            continue
        now = _reel_identity(page)
        if now and now != origin:
            return True
        if not reel_page_is_broken(page):
            return True
    return not reel_page_is_broken(page)


def browse_reels_feed(
    page: Any,
    *,
    seconds: int,
    should_stop: Callable[[], bool] | None = None,
) -> dict[str, Any]:
    """Lướt Reel bằng phím xuống. Không bấm thích, bình luận hay chia sẻ."""
    blocked = _session_block(page)
    if blocked:
        return {"error_code": blocked, "browsed_seconds": 0}
    _open_reels(page)
    if reel_page_is_broken(page):
        skip_broken_reel(page)
    _lock_user(page)
    blocked = _session_block(page)
    if blocked:
        return {"error_code": blocked, "browsed_seconds": 0}
    watched_ms = 0
    target_ms = max(0, int(seconds)) * 1000
    while watched_ms < target_ms:
        if should_stop is not None and should_stop():
            return {"error_code": "CANCELLED", "browsed_seconds": watched_ms // 1000}
        if reel_page_is_broken(page):
            skip_broken_reel(page)
        chunk = min(1000, target_ms - watched_ms)
        page.wait_for_timeout(chunk)
        watched_ms += chunk
        if watched_ms < target_ms and watched_ms % 3000 == 0:
            _next_reel(page)
            if reel_page_is_broken(page):
                skip_broken_reel(page)
    return {"error_code": "", "browsed_seconds": int(seconds)}


def _lock_user(page: Any) -> None:
    """Che trang để người dùng không bấm được trong lúc tool đang chạy."""
    try:
        from src.automation.facebook_actions import _enable_view_only_guard, register_view_only_page_hooks

        register_view_only_page_hooks(page, force=True)
        _enable_view_only_guard(page, force=True)
    except Exception:  # noqa: BLE001
        return


def _unlock_user(page: Any) -> None:
    """Mở khóa trong lúc tool bấm. Gọi lại ``_lock_user`` ngay sau đó."""
    try:
        from src.automation.facebook_actions import _disable_view_only_guard

        _disable_view_only_guard(page)
    except Exception:  # noqa: BLE001
        return


def _session_block(page: Any) -> str:
    """Checkpoint hoặc trang đăng nhập thì dừng, không bấm xác nhận."""
    url = str(getattr(page, "url", "") or "").casefold()
    if "checkpoint" in url:
        try:
            from src.services.facebook_session_recovery import try_skip_soft_facebook_checkpoint

            if try_skip_soft_facebook_checkpoint(page):
                return ""
        except Exception:
            pass
        return "CHECKPOINT"
    if "/login" in url or "login.php" in url:
        return "SESSION_EXPIRED"
    return ""


def _goto_reels_home(page: Any) -> None:
    """Mở bảng Reels toàn cục để lấy clip khác."""
    goto = getattr(page, "goto", None)
    if goto is None:
        return
    try:
        goto(_REELS_HOME, wait_until="domcontentloaded", timeout=60_000)
    except Exception:  # noqa: BLE001
        return


def _open_reels(page: Any) -> None:
    """Mở bảng Reel nếu trình duyệt chưa ở đó. Reel lỗi thì không coi là đã vào."""
    url = str(getattr(page, "url", "") or "").casefold()
    if "/reel" in url and not reel_page_is_broken(page):
        return
    _goto_reels_home(page)


def _next_reel(page: Any) -> None:
    """Chuyển Reel kế tiếp. Không click nút trên video."""
    keyboard = getattr(page, "keyboard", None)
    if keyboard is None:
        return
    try:
        keyboard.press("ArrowDown")
    except Exception:  # noqa: BLE001
        return


_INTERLEAVE_MIN_SECONDS = 6


def _action_marks_ms(target_ms: int, count: int, rng: random.Random) -> list[int]:
    """Mốc (ms) bấm từng thao tác trong lúc xem. Video ngắn thì làm sau khi xem xong."""
    if count <= 0:
        return []
    if target_ms < _INTERLEAVE_MIN_SECONDS * 1000:
        return [target_ms] * count
    marks: list[int] = []
    for index in range(count):
        low = target_ms * (index + 1) // (count + 2)
        high = target_ms * (index + 2) // (count + 2)
        marks.append(rng.randint(low, max(low, high)))
    return sorted(marks)


def _post_comment(page: Any, text: str) -> bool:
    """Mở ô bình luận dưới video, gõ và gửi. Video vẫn phát trong lúc gõ."""
    if not _is_reel(page):
        _scroll_to_comments(page)
    _pause(page, 600)
    _unlock_user(page)
    _open_comment_composer(page)
    if not _type_comment(page, text):
        return False
    _pause(page, 900)
    return True


def engage_source_video(
    page: Any,
    *,
    watch_seconds: int,
    comment: str,
    like: bool = False,
    should_stop: Callable[[], bool] | None = None,
    rng: random.Random | None = None,
) -> dict[str, Any]:
    """Xem video, trong lúc xem thì Like và bình luận — từng việc một, thứ tự ngẫu nhiên.

    Thời gian Like/bình luận tính vào thời gian xem (video vẫn phát). Video ngắn hơn
    ``_INTERLEAVE_MIN_SECONDS`` giây thì xem xong mới Like/bình luận. Không tải file video.
    """
    blocked = _session_block(page)
    if blocked:
        return {"error_code": blocked, "watched_seconds": 0, "commented": False, "liked": False}
    rnd = rng or random.Random()
    text = str(comment or "").strip()
    source_unavailable = reel_page_is_broken(page)
    if source_unavailable:
        skip_broken_reel(page)
    # Không Like/bình luận nhầm clip thay thế khi reel nguồn đã lỗi.
    actions = (
        []
        if source_unavailable
        else [name for name, on in (("like", like), ("comment", bool(text))) if on]
    )
    rnd.shuffle(actions)
    target_ms = max(0, int(watch_seconds)) * 1000
    marks = _action_marks_ms(target_ms, len(actions), rnd)
    done = {"liked": False, "commented": False}
    comment_error = ""
    _unlock_user(page)
    try:
        _press_play(page)
        _lock_user(page)
        watched_ms = 0
        while True:
            while actions and watched_ms >= marks[0]:
                marks.pop(0)
                action = actions.pop(0)
                started = time.monotonic()
                if action == "like":
                    done["liked"] = bool(like_open_video(page))
                else:
                    done["commented"] = _post_comment(page, text)
                    if not done["commented"]:
                        comment_error = "Chưa thấy ô bình luận trên video"
                _lock_user(page)
                watched_ms += int((time.monotonic() - started) * 1000)
            if watched_ms >= target_ms and not actions:
                break
            if should_stop is not None and should_stop():
                return {
                    "error_code": "CANCELLED",
                    "watched_seconds": watched_ms // 1000,
                    **done,
                }
            if reel_page_is_broken(page):
                source_unavailable = True
                actions = []
                marks = []
                skip_broken_reel(page)
            next_stop = min(target_ms, marks[0]) if marks else target_ms
            chunk = max(1, min(1000, next_stop - watched_ms))
            page.wait_for_timeout(chunk)
            watched_ms += chunk
        result: dict[str, Any] = {"error_code": "", "watched_seconds": int(watch_seconds), **done}
        if source_unavailable:
            result["error_code"] = "VIDEO_UNAVAILABLE"
            result["source_unavailable"] = True
            result["error_message"] = "Reel nguồn lỗi — đã chuyển xem reel khác"
        elif comment_error:
            result["error_message"] = comment_error
        return result
    finally:
        _lock_user(page)


_LIKE_ICON_STATE_JS = """() => {
  const mark = "M10.999.5a2.5";
  for (const path of document.querySelectorAll("svg path")) {
    if (!(path.getAttribute("d") || "").startsWith(mark)) continue;
    const host = path.closest("[role='button'], button");
    if (!host) continue;
    const label = (host.getAttribute("aria-label") || "").toLowerCase();
    if (label.startsWith("react")) continue;
    if (
      label.includes("unlike")
      || label.includes("bỏ thích")
      || host.getAttribute("aria-pressed") === "true"
    ) return "pressed";
    return "ready";
  }
  return "";
}"""
_LIKE_ICON_JS = """() => {
  const mark = "M10.999.5a2.5";
  for (const path of document.querySelectorAll("svg path")) {
    if (!(path.getAttribute("d") || "").startsWith(mark)) continue;
    const host = path.closest("[role='button'], button");
    if (!host) continue;
    const label = (host.getAttribute("aria-label") || "").toLowerCase();
    if (label.startsWith("react") || label.includes("unlike") || label.includes("bỏ thích")) continue;
    if (host.getAttribute("aria-pressed") === "true") continue;
    return host;
  }
  return null;
}"""


def like_open_video(page: Any) -> bool:
    """Bấm Like, chờ nút chuyển sang đã thích, rồi mới trả về để bình luận."""
    if _session_block(page):
        return False
    _unlock_user(page)
    _pause(page, 800)
    state = _like_icon_state(page)
    if state == "pressed":
        _lock_user(page)
        return False
    clicked = _click_like_icon(page) if state in {"ready", ""} else False
    if not clicked:
        clicked = _click_labeled_like(page)
    if not clicked:
        _lock_user(page)
        return False
    confirmed = _confirm_like(page)
    _lock_user(page)
    return confirmed


def _confirm_like(page: Any) -> bool:
    """Chờ Facebook nhận Like. Không bấm lần hai kẻo bỏ thích."""
    for _ in range(5):
        state = _like_icon_state(page)
        if state == "pressed":
            _pause(page, 800)
            return True
        if state == "":
            _pause(page, 1_000)
            return True
        _pause(page, 700)
    _pause(page, 600)
    return True


def _click_labeled_like(page: Any) -> bool:
    """Bấm nút Like có chữ khi không đọc được icon ngón tay."""
    for item in _visible_role_items(page, "button", limit=60):
        label = _label(item)
        if label.startswith("react") or not _is_video_like(label):
            continue
        try:
            pressed = str(item.get_attribute("aria-pressed") or "").casefold()
        except Exception:  # noqa: BLE001
            pressed = ""
        if pressed == "true":
            return False
        try:
            item.click(timeout=3_000)
            _pause(page, 800)
            return True
        except Exception:  # noqa: BLE001
            continue
    return False


def _like_icon_state(page: Any) -> str:
    """«ready» khi thấy icon ngón tay của nút Like. «pressed» khi đã thích."""
    runner = getattr(page, "evaluate", None)
    if runner is None:
        return ""
    try:
        return str(runner(_LIKE_ICON_STATE_JS) or "")
    except Exception:  # noqa: BLE001
        return ""


def _click_like_icon(page: Any) -> bool:
    """Bấm div role=button aria-label=Like có icon ngón tay. Không bấm React."""
    runner = getattr(page, "evaluate_handle", None)
    if runner is None:
        return False
    try:
        host = runner(_LIKE_ICON_JS)
    except Exception:  # noqa: BLE001
        return False
    if host is None:
        return False
    try:
        host.click(timeout=3_000)
        _pause(page, 800)
    except Exception:  # noqa: BLE001
        return False
    return True


def share_open_video(
    page: Any,
    *,
    kind: str,
    target_id: str = "",
    target_name: str = "",
    caption: str = "",
    like: bool = False,
) -> dict[str, Any]:
    """Bấm nút chia sẻ ngay trên video đang mở, rồi gửi lên Page hoặc một nhóm.

    Không mở tường Page để dán link thành bài mới. Video nguồn chính là bài được chia sẻ.
    """
    blocked = _session_block(page)
    if blocked:
        return {"error_code": blocked, "post_id": ""}
    _unlock_user(page)
    _dismiss_comment_editor(page)
    _pause(page, 900)
    if like:
        like_open_video(page)
        _unlock_user(page)
        _pause(page, 800)
    _close_share_sheet(page)
    if _click_video_share(page) is None:
        _lock_user(page)
        return {
            "error_code": "TIMEOUT",
            "post_id": "",
            "error_message": "Chưa thấy nút chia sẻ trên video",
        }
    _wait_share_dialog(page)
    if kind == "page" and not _page_menu_visible(page):
        _pause(page, 900)
        note = str(caption or "").strip()
        if note:
            _type_share_caption(page, note)
            _pause(page, 700)
        if _click_share_now_text(page) or _confirm_share(page, allow_now=True):
            _pause(page, 1_600)
            _close_share_sheet(page)
            sent_id = str(target_id or "").strip() or "page"
            _lock_user(page)
            return {"error_code": "", "post_id": f"{sent_id}-share", "post_url": str(getattr(page, "url", "") or "")}
        _close_share_sheet(page)
        _lock_user(page)
        return {
            "error_code": "TIMEOUT",
            "post_id": "",
            "error_message": "Chưa bấm được Share now",
        }
    specific = ("page", "trang") if kind == "page" else _GROUP_SHARE
    hints = _PAGE_SHARE if kind == "page" else _GROUP_SHARE
    option = _click_share_option(page, specific) or (None if kind == "page" else _click_share_option(page, hints))
    if option is None and kind != "page":
        _pause(page, 700)
        option = _click_share_option(page, hints)
    if option is None:
        _lock_user(page)
        return {
            "error_code": "TIMEOUT",
            "post_id": "",
            "error_message": "Chưa thấy Page hoặc nhóm trong hộp chia sẻ",
        }
    _pause(page, 400)
    token = str(target_id or "").strip()
    name = str(target_name or "").strip()
    if kind == "page":
        if (name or token) and not _choose_page(page, name, token):
            _close_share_sheet(page)
            _lock_user(page)
            return {
                "error_code": "TIMEOUT",
                "post_id": "",
                "error_message": f"Chưa thấy Page {name or token} trong hộp chia sẻ",
            }
    elif (name or token) and not _pick_share_target(page, token, name):
        _close_share_sheet(page)
        _lock_user(page)
        return {
            "error_code": "TIMEOUT",
            "post_id": "",
            "error_message": f"Chưa thấy nhóm {name or token} trong hộp chia sẻ",
        }
    note = str(caption or "").strip()
    if note:
        _type_share_caption(page, note)
    if not _confirm_share(page, allow_now=kind != "page"):
        _close_share_sheet(page)
        _lock_user(page)
        return {
            "error_code": "TIMEOUT",
            "post_id": "",
            "error_message": "Chưa bấm được nút gửi trong hộp chia sẻ",
        }
    _pause(page, 1_600)
    _close_share_sheet(page)
    _lock_user(page)
    return {"error_code": "", "post_id": f"{token or 'page'}-share", "post_url": str(getattr(page, "url", "") or "")}


def _label(item: Any) -> str:
    """Nhãn nút hoặc ô nhập. Ưu tiên aria-label vì nút phát thường không có chữ."""
    for reader in (
        lambda: item.get_attribute("aria-label"),
        lambda: item.get_attribute("aria-placeholder"),
        lambda: item.get_attribute("placeholder"),
        lambda: item.inner_text(timeout=400),
    ):
        try:
            text = str(reader() or "").strip().casefold()
        except Exception:  # noqa: BLE001
            text = ""
        if text:
            return text
    return ""


def _pause(page: Any, millis: int) -> None:
    try:
        page.wait_for_timeout(millis)
    except Exception:  # noqa: BLE001
        return


def _leading_word(label: str) -> str:
    parts = str(label or "").split()
    if not parts:
        return ""
    return parts[0].strip(".,:;!?()[]")


def _starts_with_token(label: str, tokens: tuple[str, ...]) -> bool:
    if label in tokens:
        return True
    head = _leading_word(label)
    if head in tokens:
        return True
    return any(label.startswith(token + " ") or label.startswith(token + ",") for token in tokens)


def _is_video_like(label: str) -> bool:
    """Nút Like của video. Bỏ video đã thích và nút like bình luận."""
    if not label or any(done in label for done in _LIKE_DONE):
        return False
    if any(skip in label for skip in ("comment", "bình luận", "reply", "share", "chia sẻ")):
        return False
    return _starts_with_token(label, _LIKE_WORDS)


def _is_video_share(label: str) -> bool:
    """Nút chia sẻ của video, kể cả «Share, 0 shares». Không phải mục «Share to…»."""
    if not label or any(word in label for word in _SHARE_SKIP):
        return False
    if not _starts_with_token(label, _VIDEO_SHARE):
        return False
    if any(hint in label for hint in (" to ", " lên ", " với ", "now", "ngay", "feed", "group", "nhóm", "page", "trang")):
        return False
    return len(label) <= 48


_SHARE_ICON_JS = """() => {
  const mark = "M12.863 3.156A.504.504";
  for (const path of document.querySelectorAll("svg path")) {
    if (!(path.getAttribute("d") || "").startsWith(mark)) continue;
    const svg = path.closest("svg");
    let node = svg;
    let sized = svg && svg.parentElement;
    for (let depth = 0; depth < 8 && node; depth += 1) {
      node = node.parentElement;
      if (!node) break;
      const role = (node.getAttribute("role") || "").toLowerCase();
      const label = (node.getAttribute("aria-label") || "").toLowerCase();
      if (role === "button" || node.tagName === "BUTTON") return node;
      if (label.startsWith("share") || label.startsWith("chia sẻ") || label.startsWith("chia se")) return node;
      const rect = node.getBoundingClientRect();
      if (rect.width >= 36 && rect.width <= 180 && rect.height >= 36 && rect.height <= 240) sized = node;
    }
    return sized;
  }
  return null;
}"""


def _is_reel(page: Any) -> bool:
    """Reel hiện Like, bình luận và chia sẻ thành một cột icon bên phải."""
    url = str(getattr(page, "url", "") or "").casefold()
    return "/reel/" in url or "/reels" in url


def _share_sheet_open(page: Any) -> bool:
    """Hộp chia sẻ đã mở khi thấy Share to, hoặc nút Share now / Chia sẻ ngay."""
    if _share_now_visible(page):
        return True
    for role in ("menuitem", "button", "option"):
        for item in _visible_role_items(page, role, limit=40):
            label = _label(item)
            if any(hint in label for hint in ("share to", "chia sẻ lên", "chia se len")):
                return True
    return False


def _page_menu_visible(page: Any) -> bool:
    """Hộp cũ còn mục Share to a Page. Hộp Share now không có mục này."""
    for role in ("menuitem", "button", "option", "link"):
        for item in _visible_role_items(page, role, limit=40):
            label = _label(item)
            if "share to a page" in label or "chia sẻ lên trang" in label or "chia se len trang" in label:
                return True
    return False


def _wait_share_dialog(page: Any) -> bool:
    """Chờ hộp Share hiện ra sau khi bấm mũi tên chia sẻ. Chưa thấy thì chưa bấm Share now."""
    for _ in range(5):
        if _share_sheet_open(page):
            _pause(page, 600)
            return True
        _pause(page, 600)
    return False


def _click_video_share(page: Any) -> Any | None:
    """Nút Share của video. Icon mũi tên chỉ nhận khi hộp chia sẻ mở ra."""
    host = _click_share_icon(page)
    if host is not None:
        if _wait_share_dialog(page):
            return host
    for item in _visible_role_items(page, "button", limit=80):
        if not _is_video_share(_label(item)):
            continue
        try:
            item.click(timeout=3_000)
        except Exception:  # noqa: BLE001
            continue
        _pause(page, 400)
        return item
    return None


def _click_share_icon(page: Any) -> Any | None:
    """Bấm icon mũi tên chia sẻ trên Reel khi nút không có nhãn."""
    runner = getattr(page, "evaluate_handle", None)
    if runner is None:
        return None
    try:
        host = runner(_SHARE_ICON_JS)
    except Exception:  # noqa: BLE001
        return None
    if host is None:
        return None
    try:
        host.click(timeout=3_000)
    except Exception:  # noqa: BLE001
        return None
    return host


def _click_share_option(page: Any, hints: tuple[str, ...]) -> Any | None:
    """Chọn Page hoặc nhóm trong hộp vừa mở. Không bấm lại nút chia sẻ của video."""
    for role in ("menuitem", "button", "link", "option"):
        for item in _visible_role_items(page, role, limit=80):
            label = _label(item)
            if not label or _is_video_share(label) or any(word in label for word in _SHARE_SKIP):
                continue
            if not any(hint in label for hint in hints):
                continue
            try:
                item.click(timeout=3_000)
                return item
            except Exception:  # noqa: BLE001
                return None
    return None


def _choose_page(page: Any, name: str, token: str) -> bool:
    """Chọn đúng Page. Ưu tiên tên khớp, rồi ô tìm nếu danh sách dài."""
    if _click_named(page, name, token):
        return True
    if _search_named(page, name, token):
        return True
    for item in _visible_role_items(page, "button", limit=40):
        label = _label(item)
        if not any(hint in label for hint in _POST_AS):
            continue
        try:
            item.click(timeout=3_000)
        except Exception:  # noqa: BLE001
            return False
        _pause(page, 400)
        return _click_named(page, name, token)
    return False


def _click_named(page: Any, name: str, token: str) -> bool:
    for needle in (name.casefold(), token.casefold()):
        if needle and _click_labeled_target(page, needle):
            return True
    return False


def _pick_share_target(page: Any, token: str, name: str = "") -> bool:
    """Chọn nhóm theo tên, rồi theo UID. Có ô tìm thì gõ tên nhóm."""
    if _click_named(page, name, token):
        return True
    box = _share_textbox(page)
    query = name or token
    if box is None or not query:
        return False
    try:
        box.click(timeout=3_000)
        box.type(query, delay=30)
        page.wait_for_timeout(700)
    except Exception:  # noqa: BLE001
        return False
    return _click_named(page, name, token)


def _close_share_sheet(page: Any) -> None:
    """Đóng hộp chia sẻ còn mở để lần gửi sau bấm lại được nút trên video."""
    labels = [_label(item) for item in _visible_role_items(page, "button", limit=40)]
    open_sheet = _share_now_visible(page) or any(
        label in {"close", "đóng"} or "share to" in label for label in labels
    )
    if not open_sheet:
        return
    keyboard = getattr(page, "keyboard", None)
    if keyboard is None:
        return
    try:
        keyboard.press("Escape")
    except Exception:  # noqa: BLE001
        return
    _pause(page, 200)


def _click_labeled_target(page: Any, needle: str) -> bool:
    """Bấm đúng tên. Tên khớp nguyên được chọn trước tên dài hơn có chứa cụm đó."""
    if not needle:
        return False
    ranked: list[tuple[int, int, Any]] = []
    for role in ("menuitem", "option", "link", "button", "checkbox", "radio"):
        for item in _visible_role_items(page, role, limit=80):
            label = _label(item)
            if not label or needle not in label or label in _VIDEO_SHARE:
                continue
            if label == needle:
                rank = 0
            elif label.startswith(needle + " ") or label.startswith(needle + ","):
                rank = 1
            else:
                rank = 2
            ranked.append((rank, len(label), item))
    if not ranked:
        return False
    ranked.sort(key=lambda row: (row[0], row[1]))
    try:
        ranked[0][2].click(timeout=3_000)
    except Exception:  # noqa: BLE001
        return False
    return True


def _search_named(page: Any, name: str, token: str) -> bool:
    """Gõ tên vào ô tìm của hộp chia sẻ khi Page chưa hiện trên màn hình."""
    query = (name or token or "").strip()
    if not query:
        return False
    box = None
    for item in _visible_role_items(page, "textbox", limit=12):
        label = _label(item)
        if any(hint in label for hint in ("search", "tìm", "tim kiem", "find")):
            box = item
            break
    if box is None:
        return False
    try:
        box.click(timeout=3_000)
        box.fill("")
        box.type(query[:80], delay=20)
    except Exception:  # noqa: BLE001
        return False
    _pause(page, 500)
    return _click_named(page, name, token)


def _share_textbox(page: Any) -> Any | None:
    """Ô trong hộp chia sẻ. Không dùng ô bình luận dưới video."""
    for item in _visible_role_items(page, "textbox", limit=12):
        label = _label(item)
        if any(hint in label for hint in _COMMENT_HINTS):
            continue
        return item
    return None


def _type_share_caption(page: Any, text: str) -> None:
    """Gõ nội dung kèm theo trong hộp chia sẻ. Link video không cần dán lại."""
    box = _share_textbox(page)
    if box is None:
        return
    try:
        box.click(timeout=3_000)
        box.type(text, delay=30)
    except Exception:  # noqa: BLE001
        return


_SHARE_NOW_VISIBLE_JS = """() => {
  const wanted = ["share now", "chia sẻ ngay", "chia se ngay"];
  for (const node of document.querySelectorAll("span, div, button")) {
    const text = (node.innerText || "").replace(/\\s+/g, " ").trim().toLowerCase();
    if (wanted.includes(text)) return true;
  }
  return false;
}"""


_SHARE_NOW_TEXT_JS = """() => {
  const wanted = ["share now", "chia sẻ ngay", "chia se ngay"];
  let best = null;
  let bestArea = Infinity;
  for (const node of document.querySelectorAll("span, div")) {
    const role = (node.getAttribute("role") || "").toLowerCase();
    if (role === "button") continue;
    const text = (node.innerText || "").replace(/\\s+/g, " ").trim().toLowerCase();
    if (!wanted.includes(text)) continue;
    const rect = node.getBoundingClientRect();
    const area = rect.width * rect.height;
    if (area < 10 || area >= bestArea) continue;
    best = node;
    bestArea = area;
  }
  return best;
}"""


def _share_now_visible(page: Any) -> bool:
    """Hộp Share của Reel có chữ Share now, kể cả khi chữ nằm trong span."""
    for item in _visible_role_items(page, "button", limit=40):
        if _label(item) in _SHARE_CONFIRM_ALT:
            return True
    runner = getattr(page, "evaluate", None)
    if runner is None:
        return False
    try:
        return bool(runner(_SHARE_NOW_VISIBLE_JS))
    except Exception:  # noqa: BLE001
        return False


def _click_share_now_text(page: Any) -> bool:
    """Bấm chữ Share now trong hộp Share. Nút này là span, không có role=button."""
    runner = getattr(page, "evaluate_handle", None)
    if runner is None:
        return False
    try:
        host = runner(_SHARE_NOW_TEXT_JS)
    except Exception:  # noqa: BLE001
        return False
    if host is None:
        return False
    try:
        host.click(timeout=3_000)
    except Exception:  # noqa: BLE001
        return False
    return True


def _confirm_share(page: Any, *, allow_now: bool = True) -> bool:
    """Bấm Đăng. Chia sẻ lên Page thì không dùng «Share now» kẻo gửi nhầm tường cá nhân."""
    buttons = _visible_role_items(page, "button", limit=40)
    preferred = [item for item in buttons if _label(item) in _SHARE_CONFIRM]
    alternate = [item for item in buttons if allow_now and _label(item) in _SHARE_CONFIRM_ALT]
    plain = [item for item in buttons if _label(item) in _VIDEO_SHARE]
    target = preferred[0] if preferred else (alternate[-1] if alternate else (plain[-1] if len(plain) > 1 else None))
    if target is None:
        return False
    try:
        target.click(timeout=3_000)
        page.wait_for_timeout(1_000)
        return True
    except Exception:  # noqa: BLE001
        return False


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


def _dismiss_comment_editor(page: Any) -> None:
    """Đóng bảng bình luận bằng chính icon bong bóng. Escape chỉ khi bảng còn mở."""
    if _comment_textbox(page) is None:
        return
    _click_comment_icon(page)
    _pause(page, 800)
    if _comment_textbox(page) is None:
        return
    keyboard = getattr(page, "keyboard", None)
    if keyboard is None:
        return
    try:
        keyboard.press("Escape")
    except Exception:  # noqa: BLE001
        return
    _pause(page, 200)


def _open_comment_composer(page: Any) -> None:
    """Mở ô bình luận. Reel bấm icon bong bóng trước vì nút không có chữ."""
    if _wait_comment_box(page, tries=1) is not None:
        return
    _click_comment_icon(page)
    if _wait_comment_box(page) is not None:
        return
    if _click_comment_label(page) and _wait_comment_box(page) is not None:
        return
    for item in _visible_role_items(page, "button", limit=40):
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
    _click_comment_icon(page)


def _click_comment_label(page: Any) -> bool:
    """Bấm nút có nhãn Comment hoặc Bình luận, kể cả khi không nằm trong 25 nút đầu."""
    locator = getattr(page, "locator", None)
    if locator is None:
        return False
    for label in ("Comment", "Bình luận", "Leave a comment"):
        try:
            nodes = locator(f"[role='button'][aria-label='{label}']")
            if nodes.count() == 0 or not nodes.first.is_visible():
                continue
            nodes.first.click(timeout=3_000)
            page.wait_for_timeout(700)
            return True
        except Exception:  # noqa: BLE001
            continue
    return False


def _click_comment_icon(page: Any) -> None:
    """Bấm icon bong bóng thoại trên Reel khi nút không có nhãn."""
    runner = getattr(page, "evaluate_handle", None)
    if runner is None:
        return
    try:
        host = runner(_COMMENT_ICON_JS)
    except Exception:  # noqa: BLE001
        return
    if host is None:
        return
    try:
        host.click(timeout=3_000)
        page.wait_for_timeout(900)
    except Exception:  # noqa: BLE001
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


def _wait_comment_box(page: Any, tries: int = 6) -> Any | None:
    """Đợi ô bình luận hiện sau khi bấm icon. Chưa thấy ô thì chưa gõ."""
    for _ in range(max(1, tries)):
        box = _comment_box(page)
        if box is not None:
            _pause(page, 400)
            return box
        if tries == 1:
            break
        _pause(page, 700)
    return None


def _comment_textbox(page: Any) -> Any | None:
    """Ô có chữ bình luận. Không lấy ô nhập bất kỳ trên trang."""
    for item in _visible_role_items(page, "textbox", limit=12):
        if any(hint in _label(item) for hint in _COMMENT_HINTS):
            return item
    return None


def _comment_box(page: Any) -> Any | None:
    labeled = _comment_textbox(page)
    if labeled is not None:
        return labeled
    locator = getattr(page, "locator", None)
    if locator is None:
        return None
    try:
        nodes = locator("[contenteditable='true']")
        count = min(nodes.count(), 6)
    except Exception:  # noqa: BLE001
        return None
    for index in range(count):
        item = nodes.nth(index)
        try:
            if item.is_visible():
                return item
        except Exception:  # noqa: BLE001
            continue
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
        _pause(page, 400)
        target.type(text, delay=80)
        _pause(page, 500)
        target.press("Enter")
        page.wait_for_timeout(1_200)
    except Exception:  # noqa: BLE001
        return False
    for item in _visible_role_items(page, "button", limit=25):
        if _label(item) not in _COMMENT_SUBMIT:
            continue
        try:
            item.click(timeout=3_000)
            page.wait_for_timeout(900)
        except Exception:  # noqa: BLE001
            break
        break
    _dismiss_comment_editor(page)
    return True
