"""
Theo dõi Page nguồn: phát hiện video vừa publish rồi giao cho list chờ chia sẻ.

Lần quét đầu chỉ ghi mốc (video cũ bỏ qua). Từ lần sau chỉ video nằm trên đầu trang
(trước video đã biết, tối đa ``TOP_NEW_WINDOW``) mới tính là mới — không phụ thuộc
ID lớn hơn mốc (ID 17 số / gợi ý từng chặn video mới 15–16 số). Không dùng Graph API.
"""

from __future__ import annotations

import random
import re
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Iterable

from loguru import logger

_VIDEO_ID_RE = re.compile(
    r"(?:/reel/|/videos/(?:[^/?#]+/)?|[?&]v=)(\d{10,17})(?!\d)",
    re.I,
)
MAX_SEEN_IDS = 500
# Mỗi vòng chỉ xếp tối đa N video mới (tránh flood hàng chờ); phần còn lại giữ chưa «seen» để lần sau lấy.
MAX_NEW_PER_SCAN = 8
# Độ sâu đầu trang để tìm biên video đã biết — khớp max_reels chế độ newest_only (~12).
TOP_NEW_WINDOW = 24

ScanFn = Callable[[str, dict[str, Any]], dict[str, Any]]


def video_id_from_url(url: str) -> str:
    """ID số của video/reel trong link Facebook; rỗng nếu không nhận ra."""
    match = _VIDEO_ID_RE.search(str(url or ""))
    vid = match.group(1) if match else ""
    return vid if _ok_watch_video_id(vid) else ""


def _ok_watch_video_id(raw: object) -> bool:
    """ID Facebook thật (10–17 số), loại ID ghép / nhiễu 18 số."""
    from src.services.page_video_harvest import is_real_facebook_video_id

    return is_real_facebook_video_id(raw)


def source_state_key(url: str) -> str:
    """Khóa ổn định cho mốc Page nguồn (bỏ / cuối, không phân biệt hoa thường)."""
    return str(url or "").strip().rstrip("/").casefold()


def is_watch_source_active(source: str, active_sources: Iterable[str]) -> bool:
    """
    True nếu ``source`` vẫn nằm trong list Page nguồn đang theo dõi.

    Khớp theo URL chuẩn hoặc cùng vanity/id Page — dùng khi xóa link nguồn
    để bỏ hàng chờ / không chia sẻ tiếp video từ nguồn đã gỡ.
    """
    raw = str(source or "").strip()
    if not raw:
        return False
    actives = [str(u).strip() for u in (active_sources or []) if str(u).strip()]
    if not actives:
        return False
    key = source_state_key(raw)
    if key in {source_state_key(u) for u in actives}:
        return True
    try:
        from src.services.facebook_reels_catalog import facebook_page_identity

        want = facebook_page_identity(raw)
        if not want:
            return False
        return any(facebook_page_identity(u) == want for u in actives)
    except Exception:
        return False


def parse_source_urls(raw: str) -> list[str]:
    """Danh sách link Page nguồn (mỗi dòng hoặc cách nhau dấu phẩy), bỏ trùng, giữ thứ tự."""
    from src.services.facebook_reels_catalog import facebook_page_identity

    found: list[str] = []
    seen: set[str] = set()
    for part in str(raw or "").replace(",", "\n").splitlines():
        url = part.strip()
        if not url or url.startswith("#"):
            continue
        if "facebook.com" not in url.lower():
            raise ValueError(f"Không phải link Page Facebook: {url[:80]}")
        if not url.lower().startswith("http"):
            url = "https://" + url.lstrip("/")
        url = url.rstrip("/") if url.endswith("/") else url
        if not facebook_page_identity(url):
            raise ValueError(
                f"Link không phải Page nguồn (cần facebook.com/TenPage hoặc profile.php?id=…): {url[:80]}"
            )
        key = source_state_key(url)
        if key in seen:
            continue
        seen.add(key)
        found.append(url)
    return found


def parse_minute_range(low: object, high: object, *, label: str, floor: int = 0) -> tuple[int, int]:
    """Khoảng phút «từ–đến» hợp lệ (đảo lại nếu nhập ngược)."""
    try:
        a = int(str(low).strip())
        b = int(str(high).strip())
    except (TypeError, ValueError):
        raise ValueError(f"{label}: nhập số phút") from None
    if a < floor or b < floor:
        raise ValueError(f"{label}: tối thiểu {floor} phút")
    return (a, b) if a <= b else (b, a)


@dataclass
class SourceState:
    """Mốc đã biết của một Page nguồn."""

    max_id: int = 0
    seen: list[str] = field(default_factory=list)
    baseline: bool = False
    last_scan: str = ""
    note: str = ""
    # Video nằm trên cùng lưới lần quét gần nhất — mốc «cuối» để lần sau so video mới.
    latest_id: str = ""
    page_name: str = ""
    page_id: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Dạng lưu vào settings."""
        return {
            "max_id": str(self.max_id),
            "seen": list(self.seen[-MAX_SEEN_IDS:]),
            "baseline": self.baseline,
            "last_scan": self.last_scan,
            "note": self.note,
            "latest_id": str(self.latest_id or ""),
            "page_name": str(self.page_name or ""),
            "page_id": str(self.page_id or ""),
        }

    @classmethod
    def from_dict(cls, row: object) -> "SourceState":
        """Đọc từ settings; dữ liệu hỏng thì coi như chưa có mốc."""
        if not isinstance(row, dict):
            return cls()
        try:
            max_id = int(str(row.get("max_id") or "0"))
        except ValueError:
            max_id = 0
        seen = [str(x) for x in (row.get("seen") or []) if str(x).isdigit()]
        seen = [x for x in seen if _ok_watch_video_id(x)][-MAX_SEEN_IDS:]
        max_id = _clamp_source_max_id(seen, max_id)
        latest = str(row.get("latest_id") or "").strip()
        if latest and not _ok_watch_video_id(latest):
            latest = ""
        if not latest and seen:
            # Khôi phục từ seen: ID lớn nhất cùng cụm độ dài với max_id.
            clustered = [x for x in seen if len(x) == len(str(max_id or x))]
            pick = clustered or seen
            latest = max(pick, key=lambda x: int(x))
        page_id = str(row.get("page_id") or "").strip()
        if page_id and not page_id.isdigit():
            page_id = ""
        return cls(
            max_id=max_id,
            seen=seen,
            baseline=bool(row.get("baseline")),
            last_scan=str(row.get("last_scan") or ""),
            note=str(row.get("note") or ""),
            latest_id=latest,
            page_name=str(row.get("page_name") or "").strip(),
            page_id=page_id,
        )


def source_page_display(
    url: str,
    *,
    page_name: str = "",
    page_id: str = "",
) -> str:
    """
    Nhãn Page nguồn trên Hàng chờ: ``Tên (id)`` khi đủ cả hai.

    Chỉ vanity → tên; chỉ id → id; có tên + id → ``Tên (id)``.
    """
    raw = str(url or "").strip()
    name = " ".join(str(page_name or "").split()).strip()
    pid = str(page_id or "").strip()
    vanity = ""
    try:
        from src.services.facebook_reels_catalog import facebook_page_identity

        ident = facebook_page_identity(raw) if raw else ""
    except Exception:
        ident = ""
    if ident.startswith("vanity:"):
        vanity = ident.split(":", 1)[1]
    elif ident.startswith("id:") and not pid:
        pid = ident.split(":", 1)[1]
    if not name:
        name = vanity
    if not pid and raw:
        part = raw.rstrip("/").rsplit("/", 1)[-1]
        if part.lower().startswith("profile.php"):
            m = re.search(r"id=(\d+)", raw, re.I)
            pid = m.group(1) if m else ""
        elif part.isdigit() and len(part) >= 8:
            pid = part
    if name and pid and name.casefold() != pid.casefold():
        return f"{name} ({pid})"
    if name:
        return name
    if pid:
        return pid
    if vanity:
        return vanity
    return raw[:48] if raw else ""


def _clamp_source_max_id(seen: list[str], max_id: int) -> int:
    """
    Mốc ID phải cùng cụm độ dài với video đã thấy (thường 15–16 số).

    ID 17–18 số từ reel gợi ý từng đẩy mốc quá cao → video mới thật không bao giờ
    thỏa ``id > max_id``.
    """
    valid = [x for x in seen if _ok_watch_video_id(x)]
    if max_id and not _ok_watch_video_id(str(max_id)):
        max_id = 0
    if not valid:
        return max_id if max_id and _ok_watch_video_id(str(max_id)) else 0
    from collections import Counter

    common = Counter(len(x) for x in valid).most_common(1)[0][0]
    clustered = [int(x) for x in valid if len(x) == common]
    if max_id and len(str(max_id)) == common:
        return max(max_id, max(clustered) if clustered else max_id)
    if clustered:
        return max(clustered)
    return max_id if max_id else 0


def merge_source_states(
    states: dict[str, "SourceState"],
    sources: Iterable[str],
) -> dict[str, "SourceState"]:
    """
    Gộp mốc đã lưu theo khóa chuẩn — tránh mất baseline khi link thêm/bớt dấu ``/``.
    """
    pooled: dict[str, SourceState] = {}
    for raw_key, state in (states or {}).items():
        key = source_state_key(raw_key)
        prev = pooled.get(key)
        if prev is None:
            pooled[key] = state
            continue
        # Giữ bản đã có mốc / ID cao hơn.
        if (state.baseline and not prev.baseline) or state.max_id > prev.max_id:
            pooled[key] = state
    out: dict[str, SourceState] = {}
    for url in sources:
        key = source_state_key(url)
        out[key] = pooled.get(key) or SourceState()
    for key, state in pooled.items():
        out.setdefault(key, state)
    return out


def _pairs_from_scan_urls(urls: Iterable[str]) -> list[tuple[int, str, str]]:
    """URL quét → (id số, id, link reel chuẩn), bỏ trùng / ID nhiễu."""
    pairs: list[tuple[int, str, str]] = []
    seen_order: set[str] = set()
    for url in urls:
        vid = video_id_from_url(url)
        if not vid or vid in seen_order:
            continue
        seen_order.add(vid)
        pairs.append((int(vid), vid, f"https://www.facebook.com/reel/{vid}"))
    return pairs


def _new_span(
    pairs: list[tuple[int, str, str]],
    prev_known: set[str],
) -> tuple[int, int]:
    """
    Đoạn video mới trên đầu trang: bỏ qua reel gợi ý đã biết (pinned),
    lấy các ID chưa thấy cho tới video đã biết tiếp theo (tối đa ``TOP_NEW_WINDOW``).
    """
    limit = min(len(pairs), TOP_NEW_WINDOW)
    if not prev_known:
        return 0, limit
    start = 0
    while start < limit and pairs[start][1] in prev_known:
        start += 1
    end = start
    while end < limit and pairs[end][1] not in prev_known:
        end += 1
    # Gợi ý đã biết đứng đầu: chỉ lấy đoạn mới khi phía dưới vẫn còn video đã biết
    # (tránh ID trang khác lộ khi cuộn). Đầu trang toàn ID mới thì vẫn lấy.
    if start > 0 and end >= limit:
        return start, start
    return start, end


def detect_new_videos(
    state: SourceState,
    scanned_urls: Iterable[str],
    extra_feeds: Iterable[Iterable[str]] | None = None,
) -> list[str]:
    """
    So kết quả quét với lịch sử từng Page nguồn, trả về link video mới (mới nhất trước)
    và cập nhật ``state``.

    Lần đầu (chưa có mốc): chỉ ghi lịch sử đầu trang, không trả video nào.
    Quét rỗng / không có ID hợp lệ: không ghi baseline (tránh lần sau lấy nhầm hết video cũ).

    Video vừa publish nằm trên đầu feed. Bỏ qua reel gợi ý đã biết đứng đầu lưới,
    lấy ID chưa thấy cho tới video đã biết tiếp theo (giới hạn ``TOP_NEW_WINDOW``).
    Giữ thứ tự lưới — không đảo theo số ID.

    Video mới vượt ``MAX_NEW_PER_SCAN``: xếp N cái đầu, phần còn lại **chưa** ghi vào
    ``seen`` → lần quét sau vẫn lấy được (không bỏ sót).
    ``extra_feeds`` = tab Videos — xét độc lập.
    """
    feeds = [_pairs_from_scan_urls(scanned_urls)]
    for extra in extra_feeds or []:
        extra_pairs = _pairs_from_scan_urls(extra)
        if extra_pairs:
            feeds.append(extra_pairs)

    if not any(feeds):
        # Không có ID hợp lệ → giữ nguyên lịch sử / chưa baseline.
        return []

    prev_known = set(state.seen)
    # Thu thập (feed_index, start, end, pairs) — biên video mới trên từng feed.
    spans: list[tuple[int, int, int, list[tuple[int, str, str]]]] = []
    for fi, pairs in enumerate(feeds):
        if not pairs:
            continue
        start, end = _new_span(pairs, prev_known)
        spans.append((fi, start, end, pairs))

    fresh: list[tuple[int, str, str]] = []
    if state.baseline:
        seen_fresh: set[str] = set()
        for _fi, start, end, pairs in spans:
            for row in pairs[start:end]:
                if row[1] in prev_known or row[1] in seen_fresh:
                    continue
                seen_fresh.add(row[1])
                fresh.append(row)
        # Đầu trang = mới nhất: giữ đúng thứ tự lưới (mới trước), không sort theo ID.
        fresh = fresh[:MAX_NEW_PER_SCAN]

    returned_ids = {vid for _n, vid, _u in fresh}
    known = set(state.seen)
    head_vids: list[str] = []

    for _fi, start, end, pairs in spans:
        # Đầu trang (ô mới nhất) — cập nhật latest / clamp max_id.
        for num, vid, _url in pairs[: max(end, 1)]:
            if len(head_vids) < TOP_NEW_WINDOW:
                head_vids.append(vid)
            state.max_id = max(state.max_id, num)

        if not state.baseline:
            # Lần đầu: ghi toàn bộ ID đã thấy trên feed vào lịch sử (mốc đối chiếu).
            for num, vid, _url in pairs:
                if vid not in known:
                    state.seen.append(vid)
                    known.add(vid)
                state.max_id = max(state.max_id, num)
            continue

        # Đã có mốc: chỉ ghi ID đã biết / đã xếp chia sẻ — không ghi video mới chưa xếp
        # (phần vượt MAX_NEW_PER_SCAN) để lần sau không bỏ sót.
        for idx, (num, vid, _url) in enumerate(pairs):
            if idx < start:
                # Pinned / đã biết trên đầu — chắc chắn nằm trong lịch sử.
                if vid not in known:
                    state.seen.append(vid)
                    known.add(vid)
                continue
            if start <= idx < end:
                if vid in returned_ids:
                    if vid not in known:
                        state.seen.append(vid)
                        known.add(vid)
                    state.max_id = max(state.max_id, num)
                # else: mới nhưng chưa xếp → để lần sau
                continue
            # Từ video đã biết trở xuống: ghi vào lịch sử (đối chiếu lần sau).
            if vid not in known:
                state.seen.append(vid)
                known.add(vid)

    state.seen = state.seen[-MAX_SEEN_IDS:]
    # Mốc chỉ từ đầu trang — không lấy ID nhiễu phía dưới.
    if not head_vids and feeds and feeds[0]:
        head_vids = [vid for _n, vid, _u in feeds[0][:TOP_NEW_WINDOW]]
    state.max_id = _clamp_source_max_id(head_vids, state.max_id)
    top_feed = feeds[0] if feeds else []
    if top_feed:
        state.latest_id = top_feed[0][1]
    elif head_vids:
        state.latest_id = head_vids[0]
    state.baseline = True
    return [url for _num, _vid, url in fresh]


def next_share_time(
    *,
    now: datetime,
    last_planned: datetime | None,
    gap_min: int,
    gap_max: int,
    rng: random.Random | None = None,
) -> datetime:
    """
    Giờ chia sẻ cho video mới: cách lượt trước lên cùng các Page một khoảng ngẫu nhiên ``gap_min``–``gap_max`` phút.

    Đã lâu không chia sẻ thì chạy sau 0–2 phút.
    """
    pick = rng or random.Random()
    soon = now + timedelta(seconds=pick.randint(0, 120))
    if last_planned is None:
        return soon.replace(microsecond=0)
    lo, hi = sorted((max(0, int(gap_min)), max(0, int(gap_max))))
    spaced = last_planned + timedelta(minutes=pick.randint(lo, hi))
    return max(soon, spaced).replace(microsecond=0)


def recommend_watch_settle_seconds(*, rng: random.Random | None = None) -> int:
    """
    Thời gian chờ trước lần quét đầu khi mở lại app.

    Lịch đăng đang/ sắp bận → chờ lâu hơn một chút; không thì 18–32s để 24/7 mau vào việc.
    """
    pick = rng or random.Random()
    schedule_busy = False
    try:
        from src.utils.feature_busy import schedule_load_for_account

        ready, running, due = schedule_load_for_account("")
        schedule_busy = (ready + running + due) > 0
    except Exception:  # noqa: BLE001
        schedule_busy = False
    if schedule_busy:
        return int(pick.randint(28, 45))
    return int(pick.randint(18, 32))


class SourceWatcher:
    """
    Luồng nền quét lần lượt các Page nguồn, nghỉ ngẫu nhiên ``scan_min``–``scan_max`` phút giữa các vòng.

    ``scan_fn(url, account)`` trả về ``{"ok", "items": [{"url"}], "message"}`` (như bộ quét Reels).
    ``on_new(source_url, video_urls)`` được gọi khi có video mới; ``on_state()`` khi mốc thay đổi để lưu.
    """

    def __init__(
        self,
        *,
        sources: list[str],
        account: dict[str, Any],
        states: dict[str, SourceState],
        scan_min: int,
        scan_max: int,
        scan_fn: ScanFn,
        on_new: Callable[[str, list[str]], None],
        on_state: Callable[[], None] | None = None,
        on_status: Callable[[str], None] | None = None,
        rng: random.Random | None = None,
    ) -> None:
        self.sources = list(sources)
        self.account = dict(account)
        self.states = merge_source_states(states, self.sources)
        # Đồng bộ lại dict gốc (share_tab giữ cùng reference để lưu settings).
        if states is not self.states:
            states.clear()
            states.update(self.states)
            self.states = states
        self.scan_min, self.scan_max = sorted((max(1, int(scan_min)), max(1, int(scan_max))))
        self._scan_fn = scan_fn
        self._on_new = on_new
        self._on_state = on_state or (lambda: None)
        self._on_status = on_status or (lambda _text: None)
        self._rng = rng or random.Random()
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread: threading.Thread | None = None
        self._scan_immediately = True
        self._settle_seconds = 0

    @property
    def running(self) -> bool:
        """Luồng theo dõi còn sống."""
        return self._thread is not None and self._thread.is_alive()

    def start(self, *, scan_immediately: bool = True, settle_seconds: int | None = None) -> None:
        """
        Bắt đầu theo dõi.

        ``scan_immediately=False``: lúc mở app chờ ngắn rồi mới quét — tránh đụng
        profile Firefox với lịch đăng vừa tự bật. Mặc định 18–35s (lịch bận → 28–45s).
        """
        if self.running:
            return
        self._stop.clear()
        self._scan_immediately = bool(scan_immediately)
        if settle_seconds is None:
            self._settle_seconds = (
                0 if self._scan_immediately else recommend_watch_settle_seconds(rng=self._rng)
            )
        else:
            self._settle_seconds = max(0, int(settle_seconds))
        self._thread = threading.Thread(target=self._loop, name="share-source-watch", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Dừng sau lượt quét hiện tại."""
        self._stop.set()
        self._wake.set()

    def scan_now(self) -> None:
        """Bỏ thời gian nghỉ, quét vòng kế tiếp ngay."""
        self._wake.set()

    def _wait_account_browser_idle(self, *, timeout_sec: float = 3600.0) -> bool:
        """
        Chờ tài khoản rảnh (không chia sẻ / đăng lịch) rồi mới quét Page nguồn.

        Tránh mở Firefox thứ 2 trên cùng profile. False nếu bị dừng hoặc hết giờ.
        """
        aid = str(
            self.account.get("id")
            or self.account.get("account_id")
            or ""
        ).strip()
        if not aid:
            return True
        deadline = time.monotonic() + max(30.0, float(timeout_sec))
        noted = False
        while not self._stop.is_set():
            reason = ""
            try:
                from src.utils.feature_busy import busy_reason

                reason = busy_reason(account_id=aid) or ""
            except Exception:  # noqa: BLE001
                reason = ""
            if not reason:
                return True
            if time.monotonic() >= deadline:
                self._on_status(
                    f"Chờ TK rảnh quá lâu ({reason}) — bỏ vòng quét này, thử sau."
                )
                return False
            if not noted:
                self._on_status(
                    f"TK đang {reason} — tạm dừng quét nguồn (1 browser / TK)…"
                )
                noted = True
            # Đánh thức sớm nếu user bấm dừng.
            self._wake.wait(timeout=2.0)
            self._wake.clear()
        return False

    def update_settings(
        self,
        *,
        sources: list[str] | None = None,
        scan_min: int | None = None,
        scan_max: int | None = None,
        wake: bool = True,
    ) -> bool:
        """
        Nhận cài đặt mới khi đang chạy (thêm Page nguồn, đổi khoảng quét).

        Đổi nguồn hoặc giảm khoảng quét → đánh thức nghỉ hiện tại để áp dụng ngay.
        Trả True nếu thật sự đổi nguồn / khoảng quét.
        """
        changed = False
        sources_changed = False
        old_min = self.scan_min
        if sources is not None:
            new_sources = [str(url).strip() for url in sources if str(url).strip()]
            if new_sources != list(self.sources):
                self.sources = new_sources
                merged = merge_source_states(self.states, self.sources)
                self.states.clear()
                self.states.update(merged)
                changed = True
                sources_changed = True
        lo = int(scan_min) if scan_min is not None else self.scan_min
        hi = int(scan_max) if scan_max is not None else self.scan_max
        new_min, new_max = sorted((max(1, lo), max(1, hi)))
        if (new_min, new_max) != (self.scan_min, self.scan_max):
            self.scan_min, self.scan_max = new_min, new_max
            changed = True
        if changed:
            logger.info(
                "[Theo dõi nguồn] Cập nhật live: {} Page nguồn, quét mỗi {}–{} phút.",
                len(self.sources),
                self.scan_min,
                self.scan_max,
            )
            # Nguồn mới hoặc quét mau hơn → bỏ chờ vòng hiện tại.
            if wake and (sources_changed or new_min < old_min):
                self.scan_now()
        return changed

    def scan_once(self) -> int:
        """
        Quét lần lượt từng Page nguồn theo đúng thứ tự list.

        Mỗi Page giữ lịch sử ``seen`` / ``latest_id`` riêng để đối chiếu video mới.
        """
        total = 0
        if not self.sources:
            self._on_status("Chưa có Page nguồn để quét.")
            return 0
        n_src = len(self.sources)
        for index, url in enumerate(self.sources, start=1):
            if self._stop.is_set():
                break
            # Không quét khi cùng TK đang chia sẻ / đăng lịch — tránh mở Firefox thứ 2.
            if not self._wait_account_browser_idle():
                break
            key = source_state_key(url)
            state = self.states.setdefault(key, SourceState())
            label_pre = source_page_display(
                url, page_name=state.page_name, page_id=state.page_id
            ) or url
            hist = len(state.seen)
            self._on_status(
                f"Quét {index}/{n_src}: {label_pre}"
                + (f" · đã nhớ {hist} video" if hist else " · lần đầu ghi mốc")
            )
            try:
                result = self._scan_fn(url, self.account)
            except Exception as exc:  # noqa: BLE001
                result = {"ok": False, "items": [], "message": str(exc)}
            state.last_scan = datetime.now().replace(microsecond=0).isoformat()
            if not result.get("ok"):
                state.note = f"Lỗi quét: {str(result.get('message') or '')[:200]}"
                logger.warning("[Theo dõi nguồn] {} — {}", url, state.note)
                self._on_status(f"{label_pre} — {state.note}")
                self._on_state()
                continue
            urls = [str(row.get("url") or "") for row in (result.get("items") or []) if isinstance(row, dict)]
            extra_raw = [str(u) for u in (result.get("video_urls") or []) if str(u).strip()]
            # Bỏ trùng URL đã nằm trong items (tránh đếm 2 lần cùng video).
            item_ids = {video_id_from_url(u) for u in urls}
            extra_raw = [u for u in extra_raw if video_id_from_url(u) not in item_ids]
            extra_feeds: list[list[str]] = []
            if extra_raw:
                extra_feeds.append(extra_raw)
            pname = str(result.get("page_name") or "").strip()
            pid = str(result.get("fb_page_id") or "").strip()
            if pname:
                state.page_name = pname
            if pid and pid.isdigit():
                state.page_id = pid
            first_time = not state.baseline
            n_grid = len(_pairs_from_scan_urls(urls))
            if n_grid == 0 and not extra_raw:
                state.note = (
                    "Chưa lấy được video trên Page — giữ lịch sử cũ, thử lại vòng sau."
                    if state.baseline
                    else "Chưa lấy được video — chưa ghi mốc (lần sau quét lại)."
                )
                logger.warning("[Theo dõi nguồn] {} — quét rỗng, không cập nhật mốc.", url)
                label = source_page_display(
                    url, page_name=state.page_name, page_id=state.page_id
                ) or url
                self._on_status(f"{label} — {state.note}")
                self._on_state()
                continue
            fresh = detect_new_videos(state, urls, extra_feeds=extra_feeds or None)
            n_hist = len(state.seen)
            latest = str(state.latest_id or "").strip()
            latest_hint = f" · mới nhất {latest}" if latest else ""
            hist_hint = f" · nhớ {n_hist} video"
            if first_time and state.baseline:
                state.note = (
                    f"Đã ghi mốc {n_grid} video đầu trang — bỏ qua cũ"
                    f"{latest_hint}{hist_hint}. Lần sau chỉ lấy video đăng sau."
                )
                logger.info(
                    "[Theo dõi nguồn] [{}] {} — ghi mốc {} video, latest_id={}, seen={}.",
                    index,
                    url,
                    n_grid,
                    latest or "(trống)",
                    n_hist,
                )
            elif fresh:
                state.note = f"Có {len(fresh)} video mới{latest_hint}{hist_hint}."
                logger.info(
                    "[Theo dõi nguồn] [{}] {} — {} video mới (seen={}): {}",
                    index,
                    url,
                    len(fresh),
                    n_hist,
                    ", ".join(fresh),
                )
            else:
                state.note = f"Chưa có video mới (lưới {n_grid}){latest_hint}{hist_hint}."
            label = source_page_display(
                url, page_name=state.page_name, page_id=state.page_id
            ) or url
            self._on_status(f"[{index}/{n_src}] {label} — {state.note}")
            self._on_state()
            if fresh:
                total += len(fresh)
                try:
                    self._on_new(url, fresh)
                except Exception as exc:  # noqa: BLE001
                    logger.exception("[Theo dõi nguồn] Xếp video mới vào list lỗi: {}", exc)
        return total

    def _loop(self) -> None:
        """Quét → nghỉ ngẫu nhiên → quét, tới khi bị dừng."""
        settle = int(getattr(self, "_settle_seconds", 0) or 0)
        if settle > 0 and not self._scan_immediately and not self._stop.is_set():
            at = (datetime.now() + timedelta(seconds=settle)).strftime("%H:%M:%S")
            self._on_status(
                f"Theo dõi {len(self.sources)} Page nguồn — chờ lịch đăng nhả profile "
                f"~{settle}s, quét lần đầu lúc {at}."
            )
            self._wake.clear()
            self._wake.wait(settle)
        while not self._stop.is_set():
            found = self.scan_once()
            if self._stop.is_set():
                break
            wait_min = self._rng.randint(self.scan_min, self.scan_max)
            at = (datetime.now() + timedelta(minutes=wait_min)).strftime("%H:%M")
            self._on_status(
                f"Theo dõi {len(self.sources)} Page nguồn — vòng này {found} video mới. Quét lại lúc {at}."
            )
            self._wake.clear()
            self._wake.wait(wait_min * 60)
        self._on_status("Đã tắt theo dõi Page nguồn.")
