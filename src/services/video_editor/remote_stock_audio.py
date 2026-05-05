"""
Tìm kiếm và tải âm thanh từ nguồn công khai (Openverse, Wikimedia Commons, Jamendo, Freesound).

Openverse: không cần API key (ẩn danh); ``page_size`` tối đa 20 — API trả 401 nếu vượt.
Wikimedia Commons: tìm file trong không gian File (âm thanh), không cần khóa — tuân thủ giấy phép từng file.
Jamendo: API v3.0, cần ``client_id`` (https://devportal.jamendo.com/).
Freesound: cần API key (https://freesound.org/apiv2/apply) để tìm; tải gốc cần token,
  nếu bị từ chối sẽ thử preview HQ (mp3).
"""

from __future__ import annotations

import json
import re
import shutil
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.services.video_editor.layout import ensure_video_editor_layout

# Nhiều CDN (Jamendo, Freesound…) từ chối hoặc trả 401 nếu UA quá «lạ».
DEFAULT_HTTP_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

_CONFIG_NAME = "remote_audio_config.json"

DEFAULT_REMOTE_AUDIO_CONFIG: dict[str, Any] = {
    "freesound_api_key": "",
    "jamendo_client_id": "",
    "auto_download_to_stock": True,
    "auto_download_max": 5,
    "background_fill_enabled": True,
    "background_fill_max": 8,
    "background_fill_interval_minutes": 0,
    "background_fill_topic_index": 0,
}


def remote_audio_config_file(paths: dict[str, Path] | None = None) -> Path:
    base = paths if paths is not None else ensure_video_editor_layout()
    return base["root"] / _CONFIG_NAME


def load_remote_audio_config(paths: dict[str, Path] | None = None) -> dict[str, Any]:
    out = dict(DEFAULT_REMOTE_AUDIO_CONFIG)
    p = remote_audio_config_file(paths)
    if not p.is_file():
        return out
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            out.update(raw)
    except (json.JSONDecodeError, OSError):
        pass
    out["freesound_api_key"] = str(out.get("freesound_api_key") or "")
    out["jamendo_client_id"] = str(out.get("jamendo_client_id") or "")
    out["auto_download_to_stock"] = bool(out.get("auto_download_to_stock"))
    try:
        out["auto_download_max"] = max(1, min(30, int(out.get("auto_download_max") or 5)))
    except (TypeError, ValueError):
        out["auto_download_max"] = 5
    out["background_fill_enabled"] = bool(out.get("background_fill_enabled", True))
    try:
        out["background_fill_max"] = max(1, min(25, int(out.get("background_fill_max") or 8)))
    except (TypeError, ValueError):
        out["background_fill_max"] = 8
    try:
        out["background_fill_interval_minutes"] = max(0, min(1440, int(out.get("background_fill_interval_minutes") or 0)))
    except (TypeError, ValueError):
        out["background_fill_interval_minutes"] = 0
    try:
        out["background_fill_topic_index"] = max(0, int(out.get("background_fill_topic_index") or 0))
    except (TypeError, ValueError):
        out["background_fill_topic_index"] = 0
    return out


def save_remote_audio_config(data: dict[str, Any], paths: dict[str, Path] | None = None) -> None:
    p = remote_audio_config_file(paths)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

# Nhãn hiển thị (VI) → truy vấn tìm kiếm (EN)
FREE_AUDIO_TOPIC_QUERIES: tuple[tuple[str, str], ...] = (
    ("Ambient / thư giãn", "ambient calm relaxing meditation"),
    ("Vlog / acoustic vui", "acoustic ukulele happy vlog"),
    ("Corporate / năng động", "corporate upbeat positive business"),
    ("Cinematic / hùng tráng", "cinematic epic orchestral trailer"),
    ("Nature / thiên nhiên", "nature forest birds water rain"),
    ("Electronic / synth chill", "electronic chill synth ambient"),
    ("Lofi / beat nhẹ", "lofi hip hop chill beat"),
    ("Động lực / workout", "energetic motivation workout sport"),
    ("Hài hước / quirky", "funny quirky cartoon playful"),
    ("Hùng vĩ / drone", "dark drone atmospheric tension"),
)


@dataclass
class RemoteAudioHit:
    """Một kết quả tìm kiếm có thể tải."""

    provider: str
    title: str
    creator: str
    license_: str
    license_url: str
    duration_sec: float | None
    download_url: str
    source_id: str
    attribution: str
    extra: dict[str, Any]


def _needs_freesound_api_token(url: str) -> bool:
    """
    URL cần header Authorization (Freesound API).
    Không dùng quy tắc chung «/download» — tránh nhầm CDN / host khác.
    """
    u = url.lower()
    if "cdn.freesound.org" in u or "media.freesound.org" in u:
        return False
    if "freesound.org" not in u:
        return False
    if "/apiv2/" in u and "download" in u:
        return True
    if "/sounds/" in u and "/download" in u:
        return True
    return False


def _openverse_extra_fallback_urls(raw: dict[str, Any], primary: str) -> list[str]:
    out: list[str] = []
    seen = {primary}
    for af in raw.get("alt_files") or []:
        if not isinstance(af, dict):
            continue
        u = _abs_download_url(str(af.get("url") or "").strip())
        if u and u not in seen:
            seen.add(u)
            out.append(u)
    return out


def _download_headers_for(hit: RemoteAudioHit, attempt_url: str, freesound_api_key: str) -> dict[str, str]:
    h: dict[str, str] = {
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
    }
    ul = attempt_url.lower()
    raw = hit.extra.get("raw") if isinstance(hit.extra.get("raw"), dict) else {}
    land = str(raw.get("foreign_landing_url") or "").strip()
    if hit.provider == "commons" or "upload.wikimedia.org" in ul or "upload.wikimedia.org" in hit.download_url.lower():
        fp = str(hit.extra.get("commons_file_page") or "").strip()
        if fp.lower().startswith("http"):
            h["Referer"] = fp
        else:
            h["Referer"] = "https://commons.wikimedia.org/"
    elif hit.provider == "jamendo" or "jamendo" in ul:
        surl = str(raw.get("shareurl") or "").strip()
        if surl.lower().startswith("http") and "jamendo" in surl.lower():
            h["Referer"] = surl
        elif land.lower().startswith("http") and "jamendo" in land.lower():
            h["Referer"] = land
        else:
            h["Referer"] = "https://www.jamendo.com/"
        h["Origin"] = "https://www.jamendo.com"
    elif "freesound.org" in ul or hit.provider.startswith("openverse"):
        h["Referer"] = "https://openverse.org/"
    key = str(freesound_api_key or "").strip()
    if key and _needs_freesound_api_token(attempt_url):
        h["Authorization"] = f"Token {key}"
    return h


def _refresh_openverse_hit_from_detail(hit: RemoteAudioHit) -> None:
    """
    GET /v1/audio/{uuid}/ — URL trong danh sách đôi khi lỗi thời; bản chi tiết đáng tin hơn.
    Xem https://api.openverse.org/v1/
    """
    if not str(hit.provider or "").startswith("openverse"):
        return
    raw = hit.extra.get("raw")
    if not isinstance(raw, dict):
        return
    detail_url = str(raw.get("detail_url") or "").strip()
    if not detail_url:
        return
    try:
        data = _http_json(detail_url)
    except (urllib.error.HTTPError, OSError, json.JSONDecodeError, TimeoutError):
        return
    if not isinstance(data, dict):
        return
    nu = str(data.get("url") or "").strip()
    if nu:
        hit.download_url = _abs_download_url(nu)
    primary = _abs_download_url(str(data.get("url") or hit.download_url))
    hit.extra["fallback_urls"] = _openverse_extra_fallback_urls(data, primary)
    merged = dict(raw)
    for k in ("url", "alt_files", "foreign_landing_url", "detail_url", "duration", "filetype"):
        if k in data:
            merged[k] = data[k]
    hit.extra["raw"] = merged


def _ordered_download_urls(candidates: list[str], freesound_api_key: str) -> list[str]:
    """Ưu tiên URL không cần token (CDN, Jamendo…) — tránh 401 khi chỉ có preview công khai."""
    key = str(freesound_api_key or "").strip()
    public: list[str] = []
    private: list[str] = []
    for u in candidates:
        if not u:
            continue
        if _needs_freesound_api_token(u):
            private.append(u)
        else:
            public.append(u)
    if key:
        return public + private
    return public + private


def _abs_download_url(url: str) -> str:
    u = str(url or "").strip()
    if not u:
        return u
    if u.startswith("//"):
        return "https:" + u
    if u.startswith("/"):
        return "https://freesound.org" + u
    return u


def _http_json(url: str, *, headers: dict[str, str] | None = None, timeout: int = 45) -> dict[str, Any]:
    h = {
        "User-Agent": DEFAULT_HTTP_UA,
        "Accept": "application/json",
        "Referer": "https://openverse.org/",
        **(headers or {}),
    }
    req = urllib.request.Request(url, headers=h)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
    return json.loads(raw.decode("utf-8", errors="replace"))


def _http_commons_api_json(url: str, *, timeout: int = 45) -> dict[str, Any]:
    """MediaWiki API trên commons.wikimedia.org (User-Agent theo khuyến nghị Wikimedia)."""
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": (
                "ToolFB/1.0 (video editor stock audio; +https://commons.wikimedia.org/wiki/Category:Audio_files)"
            ),
            "Accept": "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
    return json.loads(raw.decode("utf-8", errors="replace"))


def _http_download_once(url: str, dest: Path, hdr: dict[str, str], *, timeout: int) -> None:
    req = urllib.request.Request(url, headers=hdr)
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp, tmp.open("wb") as out:
            shutil.copyfileobj(resp, out)
        tmp.replace(dest)
    finally:
        tmp.unlink(missing_ok=True)


def _http_download(url: str, dest: Path, *, headers: dict[str, str] | None = None, timeout: int = 300) -> None:
    """Thử đủ header; nếu 401/403 có thể do Referer/Origin — thử lại chỉ User-Agent."""
    base = {"User-Agent": DEFAULT_HTTP_UA, **(headers or {})}
    fallbacks: list[dict[str, str]] = [base]
    if headers:
        fallbacks.append({"User-Agent": DEFAULT_HTTP_UA, "Accept": "*/*"})
    last: urllib.error.HTTPError | None = None
    for i, hdr in enumerate(fallbacks):
        try:
            _http_download_once(url, dest, hdr, timeout=timeout)
            return
        except urllib.error.HTTPError as e:
            last = e
            if e.code in (401, 403) and i < len(fallbacks) - 1:
                continue
            raise


def _slug_filename(title: str, source_id: str, suffix: str = ".mp3") -> str:
    base = re.sub(r"[^\w\s\-]+", "", title, flags=re.UNICODE)
    base = re.sub(r"[\s\-]+", "_", base).strip("_")[:60] or "audio"
    sid = re.sub(r"[^\w\-]+", "", str(source_id))[:24]
    return f"{base}_{sid}{suffix}"


def _stock_suffix_for_url(url: str) -> str:
    u = str(url or "").lower().split("?", 1)[0]
    for ext in (".flac", ".wav", ".ogg", ".oga", ".opus", ".webm", ".m4a", ".aac", ".mp3"):
        if u.endswith(ext):
            return ext
    return ".mp3"


def _commons_wiki_page_url(title: str) -> str:
    t = str(title or "").strip().replace(" ", "_")
    return "https://commons.wikimedia.org/wiki/" + urllib.parse.quote(t, safe="/:()#%,")


def search_openverse(
    query: str,
    *,
    page_size: int = 20,
    licenses: str = "cc0,by,by-sa",
) -> list[RemoteAudioHit]:
    q = str(query or "").strip()
    if not q:
        return []
    # Ẩn danh: Openverse từ chối page_size > 20 bằng HTTP 401 (không phải thiếu token).
    ps = max(5, min(20, int(page_size)))
    params = {
        "q": q,
        "page_size": str(ps),
        "license": licenses,
    }
    url = "https://api.openverse.org/v1/audio/?" + urllib.parse.urlencode(params)
    try:
        data = _http_json(url)
    except urllib.error.HTTPError as e:
        if e.code == 401:
            try:
                raw_b = e.read()
                err_j = json.loads(raw_b.decode("utf-8", errors="replace"))
                det = str((err_j.get("detail") if isinstance(err_j, dict) else "") or "")
            except (json.JSONDecodeError, TypeError, OSError):
                det = ""
            if "page_size" in det.lower() and "anonymous" in det.lower():
                raise RuntimeError(
                    "Openverse (ẩn danh): page_size không được vượt 20. "
                    "Cập nhật app hoặc giảm page_size. Chi tiết: https://api.openverse.org/v1/"
                ) from e
        if e.code == 429:
            raise RuntimeError(
                "Openverse giới hạn tần suất (HTTP 429). Đợi vài phút hoặc đổi sang nguồn Freesound (API key). "
                "Xem https://api.openverse.org/v1/"
            ) from e
        raise
    out: list[RemoteAudioHit] = []
    for r in data.get("results") or []:
        if not isinstance(r, dict):
            continue
        dl = _abs_download_url(str(r.get("url") or "").strip())
        if not dl:
            continue
        title = str(r.get("title") or "Untitled").strip()
        creator = str(r.get("creator") or "").strip()
        lic = str(r.get("license") or "").strip()
        lic_url = str(r.get("license_url") or "").strip()
        prov = str(r.get("provider") or r.get("source") or "openverse").strip()
        rid = str(r.get("id") or "").strip()
        attr = str(r.get("attribution") or "").strip()
        dur_ms = r.get("duration")
        dur_sec: float | None = None
        if dur_ms is not None:
            try:
                dur_sec = float(dur_ms) / 1000.0
            except (TypeError, ValueError):
                dur_sec = None
        if not attr and creator:
            attr = f"{title} — {creator} ({lic})"
        fallbacks = _openverse_extra_fallback_urls(r, dl)
        out.append(
            RemoteAudioHit(
                provider=f"openverse/{prov}",
                title=title,
                creator=creator,
                license_=lic,
                license_url=lic_url,
                duration_sec=dur_sec,
                download_url=dl,
                source_id=rid or dl[-40:],
                attribution=attr or title,
                extra={"raw": r, "fallback_urls": fallbacks},
            )
        )
    return out


def search_commons_audio(
    query: str,
    *,
    limit: int = 20,
    search_fetch: int = 40,
) -> list[RemoteAudioHit]:
    """
    Tìm file âm thanh trên Wikimedia Commons (không gian File), qua API ``list=search``.
    Giấy phép từng file khác nhau — cần xem trang file trên Commons.
    """
    q = str(query or "").strip()
    if not q:
        return []
    lim = max(1, min(30, int(limit)))
    sf = max(min(50, int(search_fetch)), lim)
    base = "https://commons.wikimedia.org/w/api.php"
    # Lọc file âm thanh (Commons chủ yếu là ảnh nếu không thêm filemime:audio).
    sr_q = q if "filemime:" in q.lower() else f"{q} filemime:audio"
    params = {
        "action": "query",
        "format": "json",
        "list": "search",
        "srsearch": sr_q,
        "srnamespace": "6",
        "srlimit": str(sf),
    }
    url = base + "?" + urllib.parse.urlencode(params)
    try:
        data = _http_commons_api_json(url)
    except (urllib.error.HTTPError, OSError, json.JSONDecodeError, TimeoutError):
        return []
    qobj = data.get("query") if isinstance(data.get("query"), dict) else {}
    search_res = qobj.get("search")
    if not isinstance(search_res, list) or not search_res:
        return []
    titles: list[str] = []
    for it in search_res:
        if not isinstance(it, dict):
            continue
        t = str(it.get("title") or "").strip()
        if t:
            titles.append(t)
    if not titles:
        return []
    out: list[RemoteAudioHit] = []
    batch_size = 40
    for off in range(0, len(titles), batch_size):
        if len(out) >= lim:
            break
        batch = titles[off : off + batch_size]
        q2 = urllib.parse.urlencode(
            {
                "action": "query",
                "format": "json",
                "titles": "|".join(batch),
                "prop": "imageinfo",
                "iiprop": "url|mime|size",
            }
        )
        try:
            d2 = _http_commons_api_json(base + "?" + q2)
        except (urllib.error.HTTPError, OSError, json.JSONDecodeError, TimeoutError):
            continue
        pages = (d2.get("query") or {}).get("pages") if isinstance(d2.get("query"), dict) else None
        if not isinstance(pages, dict):
            continue
        for _pid, page in pages.items():
            if len(out) >= lim:
                break
            if not isinstance(page, dict) or page.get("missing"):
                continue
            title = str(page.get("title") or "")
            infos = page.get("imageinfo")
            if not isinstance(infos, list) or not infos:
                continue
            info = infos[0]
            if not isinstance(info, dict):
                continue
            mime = str(info.get("mime") or "")
            ml = mime.lower()
            if not ml.startswith("audio/"):
                continue
            if "midi" in ml:
                continue
            dl_url = str(info.get("url") or "").strip()
            if not dl_url:
                continue
            pageid = str(page.get("pageid") or "")
            disp = title
            if disp.lower().startswith("file:"):
                disp = disp[5:]
            disp = re.sub(r"\.(ogg|oga|flac|wav|mp3|webm|opus|m4a)$", "", disp, flags=re.I).strip() or disp
            file_page = _commons_wiki_page_url(title)
            lic_hint = "Commons (xem trang file)"
            lic_url = file_page
            out.append(
                RemoteAudioHit(
                    provider="commons",
                    title=disp[:200],
                    creator="",
                    license_=lic_hint,
                    license_url=lic_url,
                    duration_sec=None,
                    download_url=dl_url,
                    source_id=pageid or disp[:24],
                    attribution=f"{disp} — Wikimedia Commons ({file_page})",
                    extra={"raw": page, "commons_file_page": file_page},
                )
            )
    return out


def search_jamendo(query: str, client_id: str, *, limit: int = 20) -> list[RemoteAudioHit]:
    """Tìm track trên Jamendo (API v3.0). Cần ``client_id`` từ https://devportal.jamendo.com/ ."""
    q = str(query or "").strip()
    cid = str(client_id or "").strip()
    if not q or not cid:
        return []
    lim = max(1, min(50, int(limit)))
    params = {
        "client_id": cid,
        "format": "json",
        "limit": str(lim),
        "search": q,
        "audioformat": "mp32",
    }
    url = "https://api.jamendo.com/v3.0/tracks/?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": DEFAULT_HTTP_UA, "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=45) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="replace"))
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"Jamendo HTTP {e.code}: kiểm tra client_id và mạng.") from e
    if not isinstance(data, dict):
        return []
    hdr = data.get("headers")
    if isinstance(hdr, dict) and str(hdr.get("status") or "").lower() == "failed":
        msg = str(hdr.get("error_message") or hdr.get("warnings") or "Jamendo API từ chối yêu cầu.")
        raise RuntimeError(f"Jamendo: {msg}")
    out: list[RemoteAudioHit] = []
    for r in data.get("results") or []:
        if not isinstance(r, dict):
            continue
        dl = str(r.get("audiodownload") or "").strip()
        if not dl:
            continue
        title = str(r.get("name") or "Untitled").strip()
        creator = str(r.get("artist_name") or "").strip()
        lic_url = str(r.get("license_ccurl") or "").strip()
        lic = lic_url or "CC — Jamendo"
        rid = str(r.get("id") or "")
        dur_sec: float | None = None
        dur = r.get("duration")
        if dur is not None:
            try:
                dur_sec = float(dur)
            except (TypeError, ValueError):
                dur_sec = None
        attr = title
        if creator:
            attr = f"{title} — {creator}"
        out.append(
            RemoteAudioHit(
                provider="jamendo",
                title=title,
                creator=creator,
                license_=lic,
                license_url=lic_url,
                duration_sec=dur_sec,
                download_url=dl,
                source_id=rid or title[:24],
                attribution=attr,
                extra={"raw": r},
            )
        )
    return out


def search_freesound(query: str, api_key: str, *, page_size: int = 25) -> list[RemoteAudioHit]:
    q = str(query or "").strip()
    key = str(api_key or "").strip()
    if not q or not key:
        return []
    fields = "id,name,username,duration,license,license_url,previews,download,attribution"
    params = {
        "query": q,
        "fields": fields,
        "page_size": str(max(5, min(50, int(page_size)))),
        "token": key,
    }
    url = "https://freesound.org/apiv2/search/text/?" + urllib.parse.urlencode(params)
    data = _http_json(url)
    out: list[RemoteAudioHit] = []
    for r in data.get("results") or []:
        if not isinstance(r, dict):
            continue
        sid = str(r.get("id") or "")
        prev = r.get("previews") if isinstance(r.get("previews"), dict) else {}
        hq = ""
        if isinstance(prev, dict):
            hq = str(prev.get("preview-hq-mp3") or "").strip()
        dlinfo = r.get("download")
        dl = ""
        if isinstance(dlinfo, str):
            dl = dlinfo.strip()
        elif isinstance(dlinfo, dict):
            dl = str(dlinfo.get("url") or "").strip()
        dl = _abs_download_url(dl)
        hq = _abs_download_url(hq)
        if not dl:
            dl = hq
        if not dl:
            continue
        name = str(r.get("name") or "Untitled").strip()
        user = str(r.get("username") or "").strip()
        lic = str(r.get("license") or "").strip()
        lic_url = str(r.get("license_url") or "").strip()
        dur = r.get("duration")
        dur_sec: float | None = None
        if dur is not None:
            try:
                dur_sec = float(dur)
            except (TypeError, ValueError):
                dur_sec = None
        attr = str(r.get("attribution") or "").strip()
        if not attr and user:
            attr = f"{name} by {user} — {lic}"
        out.append(
            RemoteAudioHit(
                provider="freesound",
                title=name,
                creator=user,
                license_=lic,
                license_url=lic_url,
                duration_sec=dur_sec,
                download_url=dl,
                source_id=sid,
                attribution=attr or name,
                extra={
                    "api_key_required": True,
                    "sound_id": sid,
                    "previews_backup": hq or None,
                    "fallback_urls": [],
                },
            )
        )
    return out


def _dedupe_hits_by_url(hits: list[RemoteAudioHit]) -> list[RemoteAudioHit]:
    seen: set[str] = set()
    out: list[RemoteAudioHit] = []
    for h in hits:
        u = str(h.download_url or "").strip()
        if not u or u in seen:
            continue
        seen.add(u)
        out.append(h)
    return out


def take_next_background_fill_topic(paths: dict[str, Path] | None = None) -> str:
    """Lấy câu truy vấn chủ đề kế tiếp (luân phiên) và lưu chỉ số vào cấu hình."""
    cfg = load_remote_audio_config(paths)
    n = len(FREE_AUDIO_TOPIC_QUERIES)
    idx = int(cfg.get("background_fill_topic_index") or 0) % n
    cfg["background_fill_topic_index"] = (idx + 1) % n
    save_remote_audio_config(cfg, paths)
    return FREE_AUDIO_TOPIC_QUERIES[idx][1]


def gather_background_fill_hits(
    query: str,
    *,
    freesound_api_key: str = "",
    jamendo_client_id: str = "",
) -> list[RemoteAudioHit]:
    """
    Gom kết quả từ Openverse + Commons (+ Jamendo / Freesound nếu có khóa).
    Lỗi từng nguồn được bỏ qua để các nguồn khác vẫn chạy.
    """
    q = str(query or "").strip()
    if not q:
        return []
    chunks: list[RemoteAudioHit] = []
    for fn in (
        lambda: search_openverse(q),
        lambda: search_commons_audio(q, limit=15),
    ):
        try:
            chunks.extend(fn())
        except Exception:
            pass
    cid = str(jamendo_client_id or "").strip()
    if cid:
        try:
            chunks.extend(search_jamendo(q, cid, limit=15))
        except Exception:
            pass
    key = str(freesound_api_key or "").strip()
    if key:
        try:
            chunks.extend(search_freesound(q, key, page_size=15))
        except Exception:
            pass
    return _dedupe_hits_by_url(chunks)


def download_hit_to_stock(
    hit: RemoteAudioHit,
    *,
    freesound_api_key: str = "",
    paths: dict[str, Path] | None = None,
) -> Path:
    """Tải file vào data/video_editor/stock_audio/. Trả về path đích."""
    if str(hit.provider or "").startswith("openverse"):
        _refresh_openverse_hit_from_detail(hit)

    dest_dir = (paths if paths is not None else ensure_video_editor_layout())["stock_audio"]
    dest_dir.mkdir(parents=True, exist_ok=True)
    suffix = _stock_suffix_for_url(hit.download_url)
    fname = _slug_filename(hit.title, hit.source_id, suffix=suffix)
    dest = dest_dir / fname
    n = 1
    while dest.is_file():
        n += 1
        dest = dest_dir / f"{dest.stem}_{n}{dest.suffix}"

    raw_candidates: list[str] = []
    seen: set[str] = set()

    def _add(u: str) -> None:
        u = _abs_download_url(str(u or "").strip())
        if u and u not in seen:
            seen.add(u)
            raw_candidates.append(u)

    _add(hit.download_url)
    if isinstance(hit.extra.get("previews_backup"), str):
        _add(hit.extra["previews_backup"])
    for fu in hit.extra.get("fallback_urls") or []:
        _add(str(fu))

    candidates = _ordered_download_urls(raw_candidates, freesound_api_key)
    if not candidates:
        raise RuntimeError("Không có URL tải hợp lệ cho bản ghi này.")

    last_http: urllib.error.HTTPError | None = None
    for i, attempt_url in enumerate(candidates):
        if not attempt_url:
            continue
        hdr = _download_headers_for(hit, attempt_url, freesound_api_key)
        try:
            _http_download(attempt_url, dest, headers=hdr if hdr else None)
            last_http = None
            break
        except urllib.error.HTTPError as e:
            last_http = e
            if i < len(candidates) - 1 and e.code in (401, 403, 404):
                continue
            if (
                e.code == 401
                and not str(freesound_api_key or "").strip()
                and candidates
                and all(_needs_freesound_api_token(u) for u in candidates)
            ):
                raise RuntimeError(
                    "HTTP 401: chỉ có link Freesound API (cần token). "
                    "Lấy API key tại https://freesound.org/apiv2/apply — dán vào «Freesound API key», «Lưu khóa», tải lại. "
                    "Hoặc dùng nguồn Openverse và chọn bản có file .mp3 trên cdn.freesound.org."
                ) from e
            raise
    if last_http is not None:
        raise last_http
    if not dest.is_file() or dest.stat().st_size < 64:
        dest.unlink(missing_ok=True)
        raise RuntimeError("Tải về không thành công (file rỗng hoặc quá nhỏ).")
    return dest
