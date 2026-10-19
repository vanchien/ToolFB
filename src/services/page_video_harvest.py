"""
Thu ID video/reel từ payload GraphQL của chính trang Facebook (session Page), không Graph API.

Dùng khi quét Content Library: lưới DOM ảo hay thiếu hàng; JSON GraphQL lúc cuộn mới đủ.
"""

from __future__ import annotations

import json
import re
import time
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import parse_qsl, urlencode

from loguru import logger

from src.services.facebook_groups.source_watch import video_id_from_url

_VIDEO_TYPENAMES = frozenset(
    {
        "video",
        "reel",
        "story",
        "videoattachment",
        "unifiedvideo",
        "nativetemplatevideo",
        "videobreakdown",
        "sharevideo",
    }
)
_VIDEO_KEYS = frozenset(
    {
        "playable_id",
        "video_id",
        "legacy_video_id",
        "videoid",
        "videoId",
    }
)
_OWNER_KEYS = ("owner", "publisher_actor", "page", "from", "actors")
_ID_RX = re.compile(r"^\d{10,17}$")
_REEL_RX = re.compile(r"/reel/(\d{10,})", re.I)
_VIDEOS_RX = re.compile(r"/videos/(?:[^/\"'?#]+/)?(\d{10,})", re.I)
_SHORT_RANGE_PRESETS = (
    "LAST_7_DAYS",
    "LAST_14_DAYS",
    "LAST_28_DAYS",
    "LAST_30_DAYS",
    "LAST_90_DAYS",
    "LAST_365_DAYS",
    "THIS_MONTH",
    "THIS_QUARTER",
    "THIS_YEAR",
    "LAST_MONTH",
    "last_7_days",
    "last_28_days",
    "last_90_days",
    "last_365_days",
    "last_7d",
    "last_28d",
    "last_90d",
    "last_365d",
)
_SINCE_KEY_RX = re.compile(
    r'("(?:since|start_time|startTime|start_date|startDate|from_time|from)"\s*:\s*"?)(\d{9,13})("?)',
    re.I,
)


def history_since_unix(*, years: int = 10, now_ts: int | None = None) -> int:
    """Unix giây: mốc bắt đầu quét lịch sử (mặc định 10 năm)."""
    now = int(now_ts if now_ts is not None else time.time())
    return now - max(1, int(years)) * 365 * 24 * 3600


def expand_lifetime_in_graphql_body(
    body: str,
    *,
    years: int = 10,
    now_ts: int | None = None,
) -> str:
    """
    Đổi preset 7/28/90 ngày → LIFETIME và lùi ``since`` về N năm.

    Áp trên POST GraphQL của Content Library để lấy video đã đăng từ rất lâu (5–10 năm).
    """
    raw = str(body or "")
    if not raw:
        return raw
    out = raw
    for preset in _SHORT_RANGE_PRESETS:
        if preset in out:
            out = out.replace(preset, "LIFETIME")
    since = str(history_since_unix(years=years, now_ts=now_ts))

    def _bump(m: re.Match[str]) -> str:
        num = int(m.group(2))
        n = num // 1000 if num > 10_000_000_000 else num
        target = int(since)
        if n > target + 86400 * 40:
            return f"{m.group(1)}{since}{m.group(3)}"
        return m.group(0)

    out = _SINCE_KEY_RX.sub(_bump, out)
    if "variables=" in raw and out == raw:
        try:
            pairs = parse_qsl(raw, keep_blank_values=True)
            new_pairs: list[tuple[str, str]] = []
            changed = False
            for key, val in pairs:
                nv = val
                for preset in _SHORT_RANGE_PRESETS:
                    if preset in nv:
                        nv = nv.replace(preset, "LIFETIME")
                nv2 = _SINCE_KEY_RX.sub(_bump, nv)
                if nv2 != val:
                    changed = True
                new_pairs.append((key, nv2))
            if changed:
                return urlencode(new_pairs, doseq=True)
        except Exception:
            return out
    return out


def graphql_body_looks_time_filtered(body: str) -> bool:
    """True nếu payload GraphQL có preset ngày / since — nên nới lifetime."""
    b = str(body or "")
    if any(p in b for p in _SHORT_RANGE_PRESETS):
        return True
    low = b.casefold()
    return "timerange" in low or "time_range" in low or "date_preset" in low or "datepreset" in low


_GRAPHQL_URL_HINTS = (
    "/api/graphql",
    "graphql",
    "graphqlbatch",
    "/ajax/bulk-route-definitions",
    "comet_req",
)
_TIME_KEYS = (
    "creation_time",
    "created_time",
    "publish_time",
    "published_time",
    "publication_time",
    "created_time_precise",
    "publish_timestamp",
    "creation_time_ms",
)
_REL_PUB = re.compile(
    r"\b(today|yesterday|hôm nay|hôm qua)\b"
    r"(?:\s*[•·]\s*|\s+at\s+|\s+lúc\s+|\s+)?"
    r"(\d{1,2}(?::\d{2})?\s*(?:AM|PM)?)?",
    re.I,
)
_ABS_PUB = re.compile(
    r"\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+"
    r"(\d{1,2})(?:,?\s*(\d{4}))?"
    r"(?:\s+at\s+(\d{1,2}:\d{2}\s*(?:AM|PM)?))?",
    re.I,
)
_ISO_PUB = re.compile(r"\b(20\d{2}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}(?::\d{2})?)?)")
_MONTHS = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}


def coerce_unix_timestamp(val: Any) -> int:
    """Đổi số epoch / ISO thành unix giây. 0 nếu không đọc được."""
    if val is None or val is False:
        return 0
    if isinstance(val, (int, float)):
        n = int(val)
        if n > 10_000_000_000:
            n //= 1000
        return n if n >= 1_000_000_000 else 0
    s = str(val).strip()
    if not s:
        return 0
    if s.isdigit():
        return coerce_unix_timestamp(int(s))
    try:
        iso = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(iso)
        return int(dt.timestamp())
    except Exception:
        return 0


def _parse_clock(raw: str) -> tuple[int, int]:
    s = str(raw or "").strip()
    m = re.match(r"(\d{1,2})(?::(\d{2}))?\s*(AM|PM)?", s, re.I)
    if not m:
        return 12, 0
    hour = int(m.group(1))
    minute = int(m.group(2) or 0)
    ap = (m.group(3) or "").upper()
    if ap == "PM" and hour < 12:
        hour += 12
    if ap == "AM" and hour == 12:
        hour = 0
    return hour, minute


def format_published_display(ts: int) -> str:
    """Chuỗi hiển thị local ``YYYY-MM-DD HH:MM``."""
    if not ts:
        return ""
    try:
        return datetime.fromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return ""


def parse_facebook_publish_stamp(value: Any, *, now: datetime | None = None) -> tuple[str, str]:
    """
    Trả ``(published_ts, published)``.

    ``published`` luôn là ``YYYY-MM-DD HH:MM`` nếu suy được mốc; không thì giữ text gốc.
    """
    ts = coerce_unix_timestamp(value)
    if ts:
        return str(ts), format_published_display(ts)
    text = str(value or "").strip()
    if not text:
        return "", ""
    base = now or datetime.now()
    m_rel = _REL_PUB.search(text)
    if m_rel:
        token = m_rel.group(1).casefold()
        clock = m_rel.group(2) or ""
        hour, minute = _parse_clock(clock)
        day = base.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if token in ("yesterday", "hôm qua"):
            day = day - timedelta(days=1)
        return str(int(day.timestamp())), format_published_display(int(day.timestamp()))
    m_abs = _ABS_PUB.search(text)
    if m_abs:
        mon = _MONTHS.get(m_abs.group(1)[:3].casefold(), 0)
        dom = int(m_abs.group(2))
        year = int(m_abs.group(3) or base.year)
        hour, minute = _parse_clock(m_abs.group(4) or "")
        if mon:
            try:
                day = datetime(year, mon, dom, hour, minute)
                if day > base + timedelta(days=2) and not m_abs.group(3):
                    day = datetime(year - 1, mon, dom, hour, minute)
                return str(int(day.timestamp())), format_published_display(int(day.timestamp()))
            except Exception:
                pass
    m_iso = _ISO_PUB.search(text)
    if m_iso:
        ts = coerce_unix_timestamp(m_iso.group(1).replace(" ", "T"))
        if ts:
            return str(ts), format_published_display(ts)
    return "", text


def extract_times_from_node(node: dict[str, Any]) -> tuple[str, str]:
    """Lấy unix + nhãn từ một object GraphQL video/story."""
    for key in _TIME_KEYS:
        if key in node:
            ts, label = parse_facebook_publish_stamp(node.get(key))
            if ts:
                return ts, label
    story = node.get("creation_story")
    if isinstance(story, dict):
        for key in _TIME_KEYS:
            if key in story:
                ts, label = parse_facebook_publish_stamp(story.get(key))
                if ts:
                    return ts, label
    return "", ""


def enrich_publish_fields(rec: dict[str, str], *, now: datetime | None = None) -> dict[str, str]:
    """Bổ sung ``published_ts`` / ``published`` (YYYY-MM-DD HH:MM) nếu thiếu."""
    ts = str(rec.get("published_ts") or "").strip()
    if ts.isdigit():
        rec["published_ts"] = ts
        rec["published"] = format_published_display(int(ts)) or str(rec.get("published") or "")
        return rec
    raw = str(rec.get("published") or rec.get("published_raw") or "")
    ts2, label = parse_facebook_publish_stamp(raw, now=now)
    if ts2:
        rec["published_ts"] = ts2
        rec["published"] = label
    return rec


def _publish_unix(rec: dict[str, str]) -> int:
    """Unix giây nếu có mốc đăng; 0 nếu thiếu (không trộn với snowflake ID)."""
    ts = str(rec.get("published_ts") or "").strip()
    if not ts.isdigit():
        return 0
    n = int(ts)
    if n > 10_000_000_000:
        n //= 1000
    return n if n > 1_000_000_000 else 0


def sort_videos_by_published(
    items: list[dict[str, str]],
    *,
    newest_first: bool = True,
) -> list[dict[str, str]]:
    """
    Sắp theo thời gian đăng thật.

    Có ``published_ts`` luôn đứng trước nhóm thiếu mốc — tránh GraphQL cũ không ngày
    bị ``reverse`` đẩy lên đầu (nhầm thành video mới nhất).
    """
    rows = [enrich_publish_fields(dict(x)) for x in items]
    dated: list[dict[str, str]] = []
    undated: list[dict[str, str]] = []
    for rec in rows:
        if _publish_unix(rec):
            dated.append(rec)
        else:
            undated.append(rec)

    def _ts_key(rec: dict[str, str]) -> int:
        return _publish_unix(rec)

    def _id_key(rec: dict[str, str]) -> int:
        vid = str(rec.get("video_id") or "").strip()
        return int(vid) if vid.isdigit() else 0

    dated.sort(key=_ts_key, reverse=newest_first)
    undated.sort(key=_id_key, reverse=newest_first)
    return dated + undated


def _kind_from_hint(hint: str) -> str:
    h = (hint or "").casefold()
    if "reel" in h or "/reel/" in h:
        return "Reel"
    if "/videos/" in h or h in {"video", "videoattachment", "unifiedvideo", "sharevideo"}:
        return "Video"
    return "Reel"


def is_real_facebook_video_id(raw: Any) -> bool:
    """True nếu là ID số Facebook thật (10–17 số) — loại ID ghép ngày giờ / chuỗi 18+ số."""
    s = str(raw or "").strip()
    if not _ID_RX.fullmatch(s):
        return False
    if len(s) >= 8:
        try:
            year = int(s[:4])
            month = int(s[4:6])
            day = int(s[6:8])
        except ValueError:
            return True
        if 2015 <= year <= 2035 and 1 <= month <= 12 and 1 <= day <= 31:
            return False
    return True


def _valid_id(raw: Any, *, exclude: set[str]) -> str:
    s = str(raw or "").strip()
    if not is_real_facebook_video_id(s):
        return ""
    if s in exclude:
        return ""
    return s


def _looks_like_thumb_uri(raw: str) -> bool:
    u = str(raw or "").strip()
    if not u.startswith("http"):
        return False
    low = u.casefold()
    if any(x in low for x in (".mp4", ".m3u8", "video/mp4", "/audio/")):
        return False
    if "scontent" in low or "fbcdn" in low:
        return True
    return any(x in low for x in (".jpg", ".jpeg", ".png", ".webp"))


def extract_thumb_from_node(node: dict[str, Any]) -> str:
    """URI ảnh thumbnail trong object GraphQL video."""
    for key in (
        "thumbnailImage",
        "preferred_thumbnail",
        "image",
        "preview_image",
        "thumbnail",
        "video_thumbnail",
        "animated_image",
        "imageHigh",
        "imageMedium",
    ):
        val = node.get(key)
        if isinstance(val, str) and _looks_like_thumb_uri(val):
            return val
        if isinstance(val, dict):
            uri = str(val.get("uri") or val.get("url") or val.get("src") or "").strip()
            if _looks_like_thumb_uri(uri):
                return uri
            nested = val.get("image")
            if isinstance(nested, dict):
                uri = str(nested.get("uri") or nested.get("url") or nested.get("src") or "").strip()
                if _looks_like_thumb_uri(uri):
                    return uri
    for val in node.values():
        if isinstance(val, str) and _looks_like_thumb_uri(val):
            return val
    return ""


def extract_duration_from_node(node: dict[str, Any]) -> str:
    """Chuẩn hoá thời lượng GraphQL thành ``m:ss``."""
    for key in ("length_in_second", "length_in_seconds", "playable_duration_in_s", "duration"):
        val = node.get(key)
        if isinstance(val, (int, float)) and val > 0:
            sec = int(val)
            return f"{sec // 60}:{sec % 60:02d}"
        if isinstance(val, str) and re.search(r"\d", val):
            return val.strip()
    for key in ("playable_duration_in_ms", "duration_ms", "length_in_milliseconds"):
        val = node.get(key)
        if isinstance(val, (int, float)) and val > 0:
            sec = int(val / 1000)
            if sec > 0:
                return f"{sec // 60}:{sec % 60:02d}"
    return ""


def extract_title_from_node(node: dict[str, Any]) -> str:
    """Tiêu đề/caption nếu có trên node video."""
    for key in ("title", "message", "name", "caption", "overlay_title", "accessibility_caption"):
        val = node.get(key)
        if isinstance(val, str):
            t = val.strip()
            if 1 < len(t) < 160 and t.casefold() not in {"video", "reel", "reels", "story"}:
                return t
        if isinstance(val, dict):
            t = str(val.get("text") or "").strip()
            if 1 < len(t) < 160 and t.casefold() not in {"video", "reel", "reels", "story"}:
                return t
    return ""


def _owner_id(obj: dict[str, Any]) -> str:
    for key in _OWNER_KEYS:
        val = obj.get(key)
        if isinstance(val, dict):
            oid = str(val.get("id") or "").strip()
            if oid.isdigit():
                return oid
        if isinstance(val, list) and val and isinstance(val[0], dict):
            oid = str(val[0].get("id") or "").strip()
            if oid.isdigit():
                return oid
    return ""


def _norm_page_vanity(raw: str) -> str:
    """Vanity Page từ username / URL — rỗng nếu không phải tên Page."""
    s = str(raw or "").strip().casefold()
    if not s:
        return ""
    m = re.search(r"facebook\.com/([^/?#]+)", s)
    if m:
        s = m.group(1).strip().casefold()
    s = s.strip("/")
    reserved = {
        "reel",
        "reels",
        "watch",
        "video",
        "videos",
        "profile.php",
        "pages",
        "photo",
        "photos",
        "share",
        "story",
        "stories",
    }
    if not s or s in reserved or s.isdigit():
        return ""
    if re.fullmatch(r"[a-z0-9.]{2,64}", s):
        return s
    return ""


def _owner_vanity(obj: dict[str, Any]) -> str:
    """Vanity/username của chủ video trong node GraphQL."""
    for key in _OWNER_KEYS:
        val = obj.get(key)
        nodes = val if isinstance(val, list) else [val]
        for node in nodes:
            if not isinstance(node, dict):
                continue
            for field in ("username", "vanity", "userVanity", "url", "profile_url"):
                got = _norm_page_vanity(str(node.get(field) or ""))
                if got:
                    return got
    return ""


def _url_belongs_to_page(url: str, *, page_id: str, vanity: str) -> bool:
    """True nếu chuỗi URL gắn Meta ID hoặc vanity của Page nguồn."""
    s = str(url or "")
    if not s:
        return False
    low = s.casefold()
    if vanity and f"/{vanity}/" in low:
        return True
    if page_id and (f"id={page_id}" in low or f"/{page_id}/" in low):
        return True
    return False


def _owner_ok(
    node: dict[str, Any],
    *,
    page_id: str,
    vanity: str,
    require_owner: bool,
) -> bool:
    """
    Video có thuộc Page nguồn không.

    ``require_owner=True`` (theo dõi chia sẻ): không owner / vanity khác → loại.
    Content Library: thiếu owner vẫn giữ (hàng thư viện của đúng Page).
    """
    oid = _owner_id(node)
    ovan = _owner_vanity(node)
    if page_id and oid:
        return oid == page_id
    if vanity and ovan:
        return ovan == vanity
    if require_owner:
        blob = " ".join(str(v) for v in node.values() if isinstance(v, str))
        return _url_belongs_to_page(blob, page_id=page_id, vanity=vanity)
    return True


def harvest_video_records_from_obj(
    obj: Any,
    *,
    fb_page_id: str = "",
    fb_page_vanity: str = "",
    trusted_surface: bool = True,
    require_owner: bool = False,
    limit: int = 2_000,
) -> list[dict[str, str]]:
    """
    Đi cây JSON GraphQL, lấy ID video của Page.

    - Luôn lấy ``playable_id`` / ``video_id``.
    - ``id`` chỉ khi ``__typename`` là Video/Reel.
    - Bỏ ID trùng Meta Page; bỏ video có ``owner.id`` khác Page (trừ bề mặt thư viện tin cậy không có owner).
    """
    page_id = str(fb_page_id or "").strip()
    vanity = _norm_page_vanity(fb_page_vanity)
    exclude = {page_id} if page_id else set()
    found: dict[str, dict[str, str]] = {}
    nodes = 0
    max_nodes = 80_000
    must_owner = bool(require_owner) or bool(page_id or vanity) and not trusted_surface

    def _add(
        vid: str,
        kind: str,
        url_hint: str = "",
        published_ts: str = "",
        published: str = "",
        thumb_url: str = "",
        title: str = "",
    ) -> None:
        if not vid or len(found) >= limit:
            return
        url = url_hint if url_hint.startswith("http") else (
            f"https://www.facebook.com/reel/{vid}" if kind == "Reel" else f"https://www.facebook.com/watch/?v={vid}"
        )
        if vid in found:
            old = found[vid]
            if published_ts and not old.get("published_ts"):
                old["published_ts"] = published_ts
                old["published"] = published or format_published_display(int(published_ts))
            if thumb_url and not old.get("thumb_url"):
                old["thumb_url"] = thumb_url
            if title and not old.get("title"):
                old["title"] = title
            return
        found[vid] = {
            "video_id": vid,
            "kind": kind,
            "url": url,
            "title": title or "",
            "published": published or "",
            "published_ts": published_ts or "",
            "thumb_url": thumb_url or "",
            "duration": "",
            "source": "network",
        }

    def _walk(node: Any, *, in_video: bool) -> None:
        nonlocal nodes
        if len(found) >= limit or nodes >= max_nodes:
            return
        if isinstance(node, dict):
            nodes += 1
            typename = str(node.get("__typename") or "").casefold()
            is_video = in_video or typename in _VIDEO_TYPENAMES or any(
                k in node for k in ("playable_id", "browser_native_hd_url", "dash_manifest")
            )
            owner_ok = _owner_ok(
                node,
                page_id=page_id,
                vanity=vanity,
                require_owner=must_owner,
            )
            pub_ts, pub_lb = extract_times_from_node(node)
            thumb = extract_thumb_from_node(node) if is_video or owner_ok else ""
            title = extract_title_from_node(node) if is_video else ""
            duration = extract_duration_from_node(node) if is_video else ""
            if is_video and owner_ok:
                for key in _VIDEO_KEYS:
                    vid = _valid_id(node.get(key), exclude=exclude)
                    if vid:
                        _add(vid, _kind_from_hint(typename or key), "", pub_ts, pub_lb, thumb, title)
                        if duration and vid in found and not found[vid].get("duration"):
                            found[vid]["duration"] = duration
                if typename in _VIDEO_TYPENAMES:
                    vid = _valid_id(node.get("id") or node.get("legacy_id"), exclude=exclude)
                    if vid:
                        _add(vid, _kind_from_hint(typename), "", pub_ts, pub_lb, thumb, title)
                        if duration and vid in found and not found[vid].get("duration"):
                            found[vid]["duration"] = duration
            elif trusted_surface:
                for key in ("playable_id", "video_id", "legacy_video_id"):
                    vid = _valid_id(node.get(key), exclude=exclude)
                    if vid and owner_ok:
                        _add(vid, _kind_from_hint(key), "", pub_ts, pub_lb, thumb, title)
                        if duration and vid in found and not found[vid].get("duration"):
                            found[vid]["duration"] = duration
            for val in node.values():
                if isinstance(val, str):
                    url_owned = _url_belongs_to_page(val, page_id=page_id, vanity=vanity)
                    keep_url = owner_ok or url_owned
                    if "/reel/" in val.casefold():
                        m = _REEL_RX.search(val)
                        if m and keep_url:
                            rid = _valid_id(m.group(1), exclude=exclude)
                            if rid:
                                _add(
                                    rid,
                                    "Reel",
                                    val if val.startswith("http") else "",
                                    pub_ts,
                                    pub_lb,
                                    thumb,
                                    title,
                                )
                    elif "/videos/" in val.casefold():
                        m = _VIDEOS_RX.search(val)
                        if m and keep_url:
                            rid = _valid_id(m.group(1), exclude=exclude)
                            if rid:
                                _add(
                                    rid,
                                    "Video",
                                    val if val.startswith("http") else "",
                                    pub_ts,
                                    pub_lb,
                                    thumb,
                                    title,
                                )
                elif isinstance(val, (dict, list)):
                    _walk(val, in_video=is_video and owner_ok)
        elif isinstance(node, list):
            nodes += 1
            for item in node:
                _walk(item, in_video=in_video)

    _walk(obj, in_video=False)
    return list(found.values())


def strip_facebook_ajax_prefix(text: str) -> str:
    """Bỏ tiền tố ``for (;;);`` / ``while(1);`` của response Facebook."""
    s = str(text or "").lstrip()
    for prefix in ("for (;;);", "while(1);", "for(;;);", "while (1);"):
        if s.startswith(prefix):
            s = s[len(prefix) :].lstrip()
            break
    return s


def iter_json_payloads(text: str) -> list[Any]:
    """Tách một hoặc nhiều JSON object trong body GraphQL/AJAX."""
    s = strip_facebook_ajax_prefix(text)
    if not s:
        return []
    out: list[Any] = []
    decoder = json.JSONDecoder()
    i = 0
    n = min(len(s), 8_000_000)
    while i < n and len(out) < 40:
        while i < n and s[i] not in "{[":
            i += 1
        if i >= n:
            break
        try:
            obj, end = decoder.raw_decode(s, i)
        except Exception:
            i += 1
            continue
        out.append(obj)
        i = max(end, i + 1)
    return out


def harvest_video_records_from_text(
    text: str,
    *,
    fb_page_id: str = "",
    fb_page_vanity: str = "",
    trusted_surface: bool = True,
    require_owner: bool = False,
) -> list[dict[str, str]]:
    """Parse JSON (hoặc chuỗi chứa JSON) rồi harvest. Fallback regex ``/reel/`` ``/videos/``."""
    raw = str(text or "").strip()
    if not raw or len(raw) < 20:
        return []
    blobs = iter_json_payloads(raw)
    if blobs:
        merged: list[dict[str, str]] = []
        for payload in blobs:
            merged.extend(
                harvest_video_records_from_obj(
                    payload,
                    fb_page_id=fb_page_id,
                    fb_page_vanity=fb_page_vanity,
                    trusted_surface=trusted_surface,
                    require_owner=require_owner,
                )
            )
        if merged:
            by: dict[str, dict[str, str]] = {}
            for rec in merged:
                vid = rec.get("video_id") or ""
                if vid and vid not in by:
                    by[vid] = rec
            return list(by.values())
    if require_owner:
        return []
    exclude = {str(fb_page_id or "").strip()} if str(fb_page_id or "").strip() else set()
    vanity = _norm_page_vanity(fb_page_vanity)
    found: dict[str, dict[str, str]] = {}
    for m in _REEL_RX.finditer(raw):
        vid = m.group(1)
        if not is_real_facebook_video_id(vid) or vid in exclude or vid in found:
            continue
        span = raw[max(0, m.start() - 180) : m.end() + 80]
        if vanity and not _url_belongs_to_page(span, page_id=str(fb_page_id or ""), vanity=vanity):
            continue
        found[vid] = {
            "video_id": vid,
            "kind": "Reel",
            "url": f"https://www.facebook.com/reel/{vid}",
            "title": "",
            "published": "",
            "source": "network",
        }
    for m in _VIDEOS_RX.finditer(raw):
        vid = m.group(1)
        if not is_real_facebook_video_id(vid) or vid in exclude or vid in found:
            continue
        span = raw[max(0, m.start() - 180) : m.end() + 80]
        if vanity and not _url_belongs_to_page(span, page_id=str(fb_page_id or ""), vanity=vanity):
            continue
        found[vid] = {
            "video_id": vid,
            "kind": "Video",
            "url": f"https://www.facebook.com/watch/?v={vid}",
            "title": "",
            "published": "",
            "source": "network",
        }
    return list(found.values())


def _title_quality(title: str) -> int:
    """Điểm tiêu đề: caption thật > «No text content» > nhiễu/trống."""
    s = str(title or "").strip()
    if not s:
        return 0
    low = s.casefold()
    if low in {"reel", "reels", "video", "videos", "--", "—", "-"}:
        return 0
    if low in {"no text content", "photo story"}:
        return 1
    return 2


def merge_video_item_lists(*groups: list[dict[str, str]]) -> list[dict[str, str]]:
    """Gộp nhiều nguồn, ưu tiên bản ghi có tiêu đề/ngày đăng; bỏ trùng theo video_id."""
    out: list[dict[str, str]] = []
    idx: dict[str, int] = {}
    for group in groups:
        for rec in group or []:
            vid = str(rec.get("video_id") or video_id_from_url(str(rec.get("url") or "")) or "").strip()
            if not vid:
                key = str(rec.get("row_sig") or rec.get("url") or "").strip()
                if not key:
                    continue
                vid = key
            if vid in idx:
                old = out[idx[vid]]
                new_title = str(rec.get("title") or "").strip()
                old_title = str(old.get("title") or "").strip()
                if _title_quality(new_title) > _title_quality(old_title):
                    old["title"] = new_title
                if rec.get("published_ts") and not str(old.get("published_ts") or "").isdigit():
                    old["published_ts"] = str(rec.get("published_ts") or "")
                    old["published"] = str(rec.get("published") or old.get("published") or "")
                elif not old.get("published") and rec.get("published"):
                    old["published"] = str(rec.get("published") or "")
                    if rec.get("published_ts"):
                        old["published_ts"] = str(rec.get("published_ts") or "")
                if rec.get("kind") and old.get("kind") != rec.get("kind"):
                    if rec.get("kind") == "Video":
                        old["kind"] = "Video"
                        old["url"] = str(rec.get("url") or old.get("url") or "")
                if rec.get("thumb_url") and not old.get("thumb_url"):
                    old["thumb_url"] = str(rec.get("thumb_url") or "")
                if rec.get("thumb_path") and not old.get("thumb_path"):
                    old["thumb_path"] = str(rec.get("thumb_path") or "")
                if rec.get("duration") and not old.get("duration"):
                    old["duration"] = str(rec.get("duration") or "")
                if rec.get("published_raw") and not old.get("published_raw"):
                    old["published_raw"] = str(rec.get("published_raw") or "")
                continue
            idx[vid] = len(out)
            row = dict(rec)
            row["video_id"] = vid
            if not row.get("url"):
                row["url"] = f"https://www.facebook.com/reel/{vid}"
            out.append(row)
    return out


def response_looks_like_facebook_graphql(url: str) -> bool:
    """True nếu URL response là GraphQL/comet của facebook.com."""
    u = str(url or "").casefold()
    if "facebook.com" not in u:
        return False
    return any(h in u for h in _GRAPHQL_URL_HINTS)


class NetworkVideoHarvest:
    """Lắng nghe ``page.on('response')`` — gom ID video khi Content Library/Reels tải thêm."""

    def __init__(
        self,
        *,
        fb_page_id: str = "",
        fb_page_vanity: str = "",
        require_owner: bool = False,
        rewrite_lifetime: bool = True,
    ) -> None:
        self.fb_page_id = str(fb_page_id or "").strip()
        self.fb_page_vanity = _norm_page_vanity(fb_page_vanity)
        self.require_owner = bool(require_owner)
        self.rewrite_lifetime = bool(rewrite_lifetime)
        self.records: dict[str, dict[str, str]] = {}
        self._attached = False

    def _harvest_kwargs(self) -> dict[str, Any]:
        return {
            "fb_page_id": self.fb_page_id,
            "fb_page_vanity": self.fb_page_vanity,
            "require_owner": self.require_owner,
            "trusted_surface": not self.require_owner,
        }

    def attach(self, page: Any) -> None:
        if self._attached:
            return
        page.on("response", self._on_response)
        if self.rewrite_lifetime:
            self._install_lifetime_route(page)
        self._attached = True

    def detach(self, page: Any) -> None:
        if not self._attached:
            return
        try:
            page.remove_listener("response", self._on_response)
        except Exception:
            pass
        try:
            page.unroute("**/*graphql*")
        except Exception:
            pass
        self._attached = False

    def _install_lifetime_route(self, page: Any) -> None:
        """Sửa POST GraphQL: 28 ngày → LIFETIME / since lùi 10 năm."""

        def _handle(route: Any) -> None:
            req = route.request
            try:
                if str(req.method or "").upper() == "POST":
                    body = req.post_data or ""
                    if graphql_body_looks_time_filtered(body):
                        new_body = expand_lifetime_in_graphql_body(body, years=10)
                        if new_body != body:
                            route.continue_(post_data=new_body)
                            return
            except Exception:
                pass
            route.continue_()

        try:
            page.route("**/*graphql*", _handle)
            page.route("**/*graphqlbatch*", _handle)
        except Exception as exc:  # noqa: BLE001
            logger.debug("[Xóa video] route lifetime graphql: {}", exc)

    def items(self) -> list[dict[str, str]]:
        return list(self.records.values())

    def ingest_text(self, text: str, *, trusted_surface: bool = True) -> int:
        """Nhập thêm từ một blob (test / HTML nhúng). Trả số ID mới."""
        added = 0
        kw = self._harvest_kwargs()
        kw["trusted_surface"] = bool(trusted_surface) and not self.require_owner
        for rec in harvest_video_records_from_text(text, **kw):
            vid = rec.get("video_id") or ""
            if not vid or vid in self.records:
                continue
            self.records[vid] = rec
            added += 1
        return added

    def _on_response(self, resp: Any) -> None:
        try:
            url = str(getattr(resp, "url", "") or "")
            if not response_looks_like_facebook_graphql(url):
                return
            status = int(getattr(resp, "status", 0) or 0)
            if status != 200:
                return
            body = ""
            try:
                data = resp.json()
            except Exception:
                data = None
            if data is not None:
                n = 0
                for rec in harvest_video_records_from_obj(data, **self._harvest_kwargs()):
                    vid = rec.get("video_id") or ""
                    if vid and vid not in self.records:
                        self.records[vid] = rec
                        n += 1
                if n:
                    logger.debug("[Xóa video] GraphQL json +{} video (tổng {}).", n, len(self.records))
                return
            body = resp.text()
        except Exception:
            return
        if not body or len(body) < 40 or len(body) > 8_000_000:
            return
        try:
            n = self.ingest_text(body, trusted_surface=True)
            if n:
                logger.debug("[Xóa video] GraphQL +{} video (tổng {}).", n, len(self.records))
        except Exception as exc:  # noqa: BLE001
            logger.debug("[Xóa video] harvest graphql: {}", exc)
