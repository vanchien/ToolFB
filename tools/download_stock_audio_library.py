#!/usr/bin/env python3
"""
Tải mẫu âm thanh vào thư viện cục bộ (data/video_editor/stock_audio/).

Nguồn (API / chính sách hợp lệ):
- Wikimedia Commons — danh mục «Audio files» qua MediaWiki API
  (https://commons.wikimedia.org/wiki/Category:Audio_files).
- Jamendo — API v3.0, cần client_id (https://devportal.jamendo.com/).
- Freesound — apiv2, cần token (https://freesound.org/apiv2/apply).

Chạy từ thư mục gốc repo:
  python tools/download_stock_audio_library.py --sources commons --limit 10
  python tools/download_stock_audio_library.py --sources jamendo --jamendo-client-id YOUR_ID --limit 15
  python tools/download_stock_audio_library.py --sources freesound --freesound-key YOUR_KEY --limit 10

Biến môi trường (tuỳ chọn): JAMENDO_CLIENT_ID, FREESOUND_API_KEY.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

# Đảm bảo import src.* khi chạy python tools/...
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.services.video_editor.layout import ensure_video_editor_layout
from src.services.video_editor.remote_stock_audio import (  # noqa: E402
    DEFAULT_HTTP_UA,
    _http_download,
    _slug_filename,
    download_hit_to_stock,
    search_freesound,
)

COMMONS_UA = (
    "ToolFB-audio-library/1.0 (local stock downloader; respects Wikimedia User-Agent policy; "
    "+https://commons.wikimedia.org/)"
)

_AUDIO_MIME_PREFIX = "audio/"


def _http_json_commons(url: str, *, timeout: int = 60) -> dict:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": COMMONS_UA,
            "Accept": "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8", errors="replace"))


def _dest_suffix_from_mime(mime: str, url: str) -> str:
    m = (mime or "").lower().split(";", 1)[0].strip()
    ul = url.lower().split("?", 1)[0]
    if m == "audio/mpeg" or ul.endswith(".mp3"):
        return ".mp3"
    if m in ("audio/ogg", "application/ogg") or ul.endswith(".ogg") or ul.endswith(".oga"):
        return ".ogg"
    if "flac" in m or ul.endswith(".flac"):
        return ".flac"
    if "wav" in m or ul.endswith(".wav"):
        return ".wav"
    if "opus" in m or ul.endswith(".opus"):
        return ".opus"
    if ul.endswith(".webm"):
        return ".webm"
    return ".bin"


def _safe_title_from_commons_page_title(title: str) -> str:
    t = title
    if t.lower().startswith("file:"):
        t = t[5:]
    return re.sub(r"\.(ogg|oga|opus|flac|wav|mp3|webm)$", "", t, flags=re.I)


def download_commons_category_audio(
    *,
    dest_dir: Path,
    limit: int,
    category: str,
    max_bytes: int,
    delay_sec: float,
) -> int:
    """
    Duyệt Category trên Commons (namespace File), chỉ tải mime audio/*, kích thước <= max_bytes.
    """
    base = "https://commons.wikimedia.org/w/api.php"
    params: dict[str, str | int] = {
        "action": "query",
        "format": "json",
        "generator": "categorymembers",
        "gcmtitle": category,
        "gcmnamespace": "6",
        "gcmlimit": "50",
        "gcmtype": "file",
        "prop": "imageinfo",
        "iiprop": "url|mime|size",
    }
    cont: dict[str, str] = {}
    saved = 0
    batches = 0
    seen_urls: set[str] = set()

    while saved < limit and batches < 400:
        batches += 1
        q = {**params, **cont}
        url = base + "?" + urllib.parse.urlencode(q)
        try:
            data = _http_json_commons(url)
        except (urllib.error.HTTPError, urllib.error.URLError, OSError, json.JSONDecodeError) as e:
            print(f"[commons] Lỗi API: {e}", file=sys.stderr)
            break

        pages = (data.get("query") or {}).get("pages") or {}
        if not isinstance(pages, dict):
            pages = {}

        for _pid, page in pages.items():
            if saved >= limit:
                break
            if not isinstance(page, dict):
                continue
            title = str(page.get("title") or "")
            infos = page.get("imageinfo")
            if not isinstance(infos, list) or not infos:
                continue
            info = infos[0]
            if not isinstance(info, dict):
                continue
            mime = str(info.get("mime") or "")
            dl_url = str(info.get("url") or "").strip()
            if not dl_url or dl_url in seen_urls:
                continue
            try:
                sz = int(info.get("size") or 0)
            except (TypeError, ValueError):
                sz = 0
            if sz > max_bytes > 0:
                continue
            if not mime.lower().startswith(_AUDIO_MIME_PREFIX):
                continue

            seen_urls.add(dl_url)
            label = _safe_title_from_commons_page_title(title)
            sid = str(page.get("pageid") or label[:12])
            suf = _dest_suffix_from_mime(mime, dl_url)
            fname = _slug_filename(label, sid, suffix=suf)
            dest = dest_dir / fname
            n = 1
            while dest.is_file():
                n += 1
                dest = dest_dir / f"{dest.stem}_{n}{dest.suffix}"
            try:
                _http_download(
                    dl_url,
                    dest,
                    headers={"User-Agent": COMMONS_UA, "Accept": "*/*"},
                    timeout=300,
                )
                if dest.is_file() and dest.stat().st_size >= 64:
                    print(f"[commons] OK {dest.name} ({mime})")
                    saved += 1
                else:
                    dest.unlink(missing_ok=True)
            except (urllib.error.HTTPError, OSError, TimeoutError) as e:
                dest.unlink(missing_ok=True)
                print(f"[commons] Bỏ qua {title}: {e}", file=sys.stderr)

            time.sleep(delay_sec)

        if "continue" in data and isinstance(data["continue"], dict):
            cont = {str(k): str(v) for k, v in data["continue"].items()}
        else:
            break

    return saved


def download_jamendo_tracks(
    *,
    client_id: str,
    dest_dir: Path,
    limit: int,
    delay_sec: float,
) -> int:
    cid = str(client_id or "").strip()
    if not cid:
        print("[jamendo] Thiếu client_id — bỏ qua. Đăng ký tại https://devportal.jamendo.com/", file=sys.stderr)
        return 0
    api = "https://api.jamendo.com/v3.0/tracks/?" + urllib.parse.urlencode(
        {
            "client_id": cid,
            "format": "json",
            "limit": str(max(1, min(100, limit))),
            "audioformat": "mp32",
        }
    )
    try:
        req = urllib.request.Request(
            api,
            headers={"User-Agent": DEFAULT_HTTP_UA, "Accept": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="replace"))
    except (urllib.error.HTTPError, urllib.error.URLError, OSError, json.JSONDecodeError) as e:
        print(f"[jamendo] Lỗi API: {e}", file=sys.stderr)
        return 0
    results = data.get("results") if isinstance(data, dict) else None
    if not isinstance(results, list):
        return 0
    saved = 0
    for r in results:
        if saved >= limit:
            break
        if not isinstance(r, dict):
            continue
        dl = str(r.get("audiodownload") or "").strip()
        if not dl:
            continue
        tid = str(r.get("id") or "")
        name = str(r.get("name") or "track").strip()
        fname = _slug_filename(name, tid or "jam", suffix=".mp3")
        dest = dest_dir / fname
        n = 1
        while dest.is_file():
            n += 1
            dest = dest_dir / f"{dest.stem}_{n}{dest.suffix}"
        try:
            hdr = {
                "User-Agent": DEFAULT_HTTP_UA,
                "Accept": "*/*",
                "Referer": "https://www.jamendo.com/",
                "Origin": "https://www.jamendo.com",
            }
            land = str(r.get("shareurl") or "").strip()
            if land.lower().startswith("http") and "jamendo.com" in land.lower():
                hdr["Referer"] = land
            _http_download(dl, dest, headers=hdr, timeout=300)
            if dest.is_file() and dest.stat().st_size >= 64:
                print(f"[jamendo] OK {dest.name}")
                saved += 1
            else:
                dest.unlink(missing_ok=True)
        except (urllib.error.HTTPError, OSError, TimeoutError) as e:
            dest.unlink(missing_ok=True)
            print(f"[jamendo] Bỏ qua {name}: {e}", file=sys.stderr)
        time.sleep(delay_sec)
    return saved


def download_freesound_batch(
    *,
    api_key: str,
    query: str,
    dest_dir: Path,
    limit: int,
    delay_sec: float,
) -> int:
    key = str(api_key or "").strip()
    if not key:
        print(
            "[freesound] Thiếu API key — bỏ qua. https://freesound.org/apiv2/apply",
            file=sys.stderr,
        )
        return 0
    paths = ensure_video_editor_layout()
    paths = {**paths, "stock_audio": dest_dir}
    hits = search_freesound(query, key, page_size=max(5, min(50, limit)))
    saved = 0
    for hit in hits:
        if saved >= limit:
            break
        try:
            out = download_hit_to_stock(hit, freesound_api_key=key, paths=paths)
            print(f"[freesound] OK {out.name}")
            saved += 1
        except Exception as e:
            print(f"[freesound] Bỏ qua {hit.title!r}: {e}", file=sys.stderr)
        time.sleep(delay_sec)
    return saved


def _configure_stdio_utf8() -> None:
    for stream in (sys.stdout, sys.stderr):
        reconf = getattr(stream, "reconfigure", None)
        if callable(reconf):
            try:
                reconf(encoding="utf-8", errors="replace")
            except Exception:
                pass


def main() -> int:
    _configure_stdio_utf8()
    ap = argparse.ArgumentParser(description="Tải âm thanh vào data/video_editor/stock_audio/.")
    ap.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Thư mục đích (mặc định: data/video_editor/stock_audio).",
    )
    ap.add_argument(
        "--sources",
        default="commons",
        help="Danh sách: commons,jamendo,freesound (phân tách dấu phẩy). Mặc định: commons.",
    )
    ap.add_argument("--limit", type=int, default=15, help="Số file tối đa mỗi nguồn (mặc định 15).")
    ap.add_argument(
        "--commons-category",
        default="Category:Audio_files",
        help="Tiêu đề đầy đủ danh mục Commons (mặc định Category:Audio_files).",
    )
    ap.add_argument(
        "--commons-max-mb",
        type=int,
        default=80,
        help="Bỏ qua file Commons lớn hơn N MB (0 = không giới hạn). Mặc định 80.",
    )
    ap.add_argument("--delay", type=float, default=0.4, help="Giây nghỉ giữa các lần tải (mặc định 0.4).")
    ap.add_argument("--jamendo-client-id", default=os.environ.get("JAMENDO_CLIENT_ID", ""))
    ap.add_argument("--freesound-key", default=os.environ.get("FREESOUND_API_KEY", ""))
    ap.add_argument("--freesound-query", default="ambient nature", help="Từ khóa tìm Freesound.")
    args = ap.parse_args()

    dest = args.out
    if dest is None:
        dest = ensure_video_editor_layout()["stock_audio"]
    dest.mkdir(parents=True, exist_ok=True)
    dest = dest.resolve()
    print(f"Thư mục đích: {dest}")

    max_bytes = 0 if int(args.commons_max_mb or 0) <= 0 else int(args.commons_max_mb) * 1024 * 1024
    names = {x.strip().lower() for x in str(args.sources).split(",") if x.strip()}
    total = 0
    lim = max(1, int(args.limit))

    if "commons" in names:
        total += download_commons_category_audio(
            dest_dir=dest,
            limit=lim,
            category=str(args.commons_category),
            max_bytes=max_bytes,
            delay_sec=float(args.delay),
        )
    if "jamendo" in names:
        jid = str(args.jamendo_client_id or "").strip()
        total += download_jamendo_tracks(
            client_id=jid,
            dest_dir=dest,
            limit=lim,
            delay_sec=float(args.delay),
        )
    if "freesound" in names:
        total += download_freesound_batch(
            api_key=str(args.freesound_key or ""),
            query=str(args.freesound_query or "ambient"),
            dest_dir=dest,
            limit=lim,
            delay_sec=float(args.delay),
        )

    print(f"Hoàn tất. Đã tải (ước lượng theo nguồn): {total} file.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
