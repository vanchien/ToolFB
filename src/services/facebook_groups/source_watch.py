"""
Theo dõi Page nguồn: phát hiện video vừa publish rồi giao cho list chờ chia sẻ.

Lần quét đầu chỉ ghi mốc (video cũ bỏ qua). Từ lần sau chỉ video nằm gần đầu trang
(``TOP_NEW_WINDOW``) với ID lớn hơn mốc mới tính là mới — tránh nhận nhầm ID lộ khi
cuộn sâu / JSON gợi ý / tab Videos. Không dùng Graph API: quét bằng profile tài khoản.
"""

from __future__ import annotations

import random
import re
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Iterable

from loguru import logger

_VIDEO_ID_RE = re.compile(r"(?:/reel/|/videos/(?:[^/?#]+/)?|[?&]v=)(\d{8,})", re.I)
MAX_SEEN_IDS = 400
MAX_NEW_PER_SCAN = 5
# Chỉ xét video nằm gần đầu trang — vừa publish luôn ở trên; ID lộ khi cuộn sâu / gợi ý không tính.
TOP_NEW_WINDOW = 12

ScanFn = Callable[[str, dict[str, Any]], dict[str, Any]]


def video_id_from_url(url: str) -> str:
    """ID số của video/reel trong link Facebook; rỗng nếu không nhận ra."""
    match = _VIDEO_ID_RE.search(str(url or ""))
    return match.group(1) if match else ""


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

    def to_dict(self) -> dict[str, Any]:
        """Dạng lưu vào settings."""
        return {
            "max_id": str(self.max_id),
            "seen": list(self.seen[-MAX_SEEN_IDS:]),
            "baseline": self.baseline,
            "last_scan": self.last_scan,
            "note": self.note,
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
        return cls(
            max_id=max_id,
            seen=seen[-MAX_SEEN_IDS:],
            baseline=bool(row.get("baseline")),
            last_scan=str(row.get("last_scan") or ""),
            note=str(row.get("note") or ""),
        )


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


def detect_new_videos(state: SourceState, scanned_urls: Iterable[str]) -> list[str]:
    """
    So kết quả quét với mốc của Page nguồn, trả về link video mới (cũ → mới) và cập nhật ``state``.

    Lần đầu (chưa có mốc): chỉ ghi mốc, không trả video nào.

    Video vừa publish luôn nằm **trên** các video đã biết. Vì vậy chỉ xét đoạn từ đầu list
    đến trước video đã biết đầu tiên (giới hạn ``TOP_NEW_WINDOW``). ID nhiễu phía dưới /
    trang khác không nâng mốc và không vào list chia sẻ.
    """
    pairs: list[tuple[int, str, str]] = []
    seen_order: set[str] = set()
    for url in scanned_urls:
        vid = video_id_from_url(url)
        if not vid or vid in seen_order:
            continue
        seen_order.add(vid)
        pairs.append((int(vid), vid, f"https://www.facebook.com/reel/{vid}"))

    prev_known = set(state.seen)
    # Cắt tại video đã biết đầu tiên — mọi thứ phía dưới không phải «mới trên đầu trang».
    cut = min(len(pairs), TOP_NEW_WINDOW)
    if prev_known:
        for index, (_num, vid, _url) in enumerate(pairs):
            if index >= TOP_NEW_WINDOW:
                cut = TOP_NEW_WINDOW
                break
            if vid in prev_known:
                cut = index
                break
    head = pairs[:cut]
    tail = pairs[cut:]

    fresh: list[tuple[int, str, str]] = []
    if state.baseline:
        candidates = [p for p in head if p[1] not in prev_known and p[0] > state.max_id]
        candidates.sort(key=lambda row: row[0])
        fresh = candidates[-MAX_NEW_PER_SCAN:]

    known = set(state.seen)
    for num, vid, _url in head:
        if vid not in known:
            state.seen.append(vid)
            known.add(vid)
        state.max_id = max(state.max_id, num)
    for _num, vid, _url in tail:
        if vid not in known:
            state.seen.append(vid)
            known.add(vid)
    state.seen = state.seen[-MAX_SEEN_IDS:]
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

    @property
    def running(self) -> bool:
        """Luồng theo dõi còn sống."""
        return self._thread is not None and self._thread.is_alive()

    def start(self, *, scan_immediately: bool = True) -> None:
        """
        Bắt đầu theo dõi.

        ``scan_immediately=False``: lúc mở app chờ hết khoảng quét rồi mới quét —
        tránh đụng profile Firefox với lịch đăng vừa tự bật.
        """
        if self.running:
            return
        self._stop.clear()
        self._scan_immediately = bool(scan_immediately)
        self._thread = threading.Thread(target=self._loop, name="share-source-watch", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Dừng sau lượt quét hiện tại."""
        self._stop.set()
        self._wake.set()

    def scan_now(self) -> None:
        """Bỏ thời gian nghỉ, quét vòng kế tiếp ngay."""
        self._wake.set()

    def update_settings(
        self,
        *,
        sources: list[str] | None = None,
        scan_min: int | None = None,
        scan_max: int | None = None,
    ) -> bool:
        """
        Nhận cài đặt mới khi đang chạy (thêm Page nguồn, đổi khoảng quét).

        Vòng nghỉ hiện tại vẫn dùng khoảng cũ; vòng sau dùng giá trị mới.
        Trả True nếu thật sự đổi nguồn / khoảng quét (tránh log lặp lúc mở app).
        """
        changed = False
        if sources is not None:
            new_sources = [str(url).strip() for url in sources if str(url).strip()]
            if new_sources != list(self.sources):
                self.sources = new_sources
                merged = merge_source_states(self.states, self.sources)
                self.states.clear()
                self.states.update(merged)
                changed = True
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
        return changed

    def scan_once(self) -> int:
        """Quét mọi Page nguồn một lượt; trả về số video mới tìm thấy."""
        total = 0
        if not self.sources:
            self._on_status("Chưa có Page nguồn để quét.")
            return 0
        for index, url in enumerate(self.sources, start=1):
            if self._stop.is_set():
                break
            self._on_status(f"Đang quét Page nguồn {index}/{len(self.sources)}: {url}")
            key = source_state_key(url)
            state = self.states.setdefault(key, SourceState())
            try:
                result = self._scan_fn(url, self.account)
            except Exception as exc:  # noqa: BLE001
                result = {"ok": False, "items": [], "message": str(exc)}
            state.last_scan = datetime.now().replace(microsecond=0).isoformat()
            if not result.get("ok"):
                state.note = f"Lỗi quét: {str(result.get('message') or '')[:200]}"
                logger.warning("[Theo dõi nguồn] {} — {}", url, state.note)
                self._on_status(state.note)
                self._on_state()
                continue
            urls = [str(row.get("url") or "") for row in (result.get("items") or []) if isinstance(row, dict)]
            first_time = not state.baseline
            fresh = detect_new_videos(state, urls)
            if first_time:
                state.note = f"Đã ghi mốc {len(urls)} video cũ (bỏ qua)."
                logger.info("[Theo dõi nguồn] {} — ghi mốc {} video cũ, chỉ chia sẻ video đăng sau.", url, len(urls))
            elif fresh:
                state.note = f"Có {len(fresh)} video mới."
                logger.info("[Theo dõi nguồn] {} — {} video mới: {}", url, len(fresh), ", ".join(fresh))
            else:
                state.note = f"Chưa có video mới (đã thấy {len(urls)} reel)."
            self._on_status(f"{url} — {state.note}")
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
        if not self._scan_immediately and not self._stop.is_set():
            wait_min = self._rng.randint(self.scan_min, self.scan_max)
            at = (datetime.now() + timedelta(minutes=wait_min)).strftime("%H:%M")
            self._on_status(
                f"Theo dõi {len(self.sources)} Page nguồn — chờ lịch đăng ổn định, quét lần đầu lúc {at}."
            )
            self._wake.clear()
            self._wake.wait(wait_min * 60)
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
