"""
Thu thập link video từ profile TikTok ``https://www.tiktok.com/@user``.

yt-dlp thường gãy ở extractor kênh (``Unable to extract secondary user ID``).
Dùng Playwright mở trang, đọc JSON/href và cuộn để lấy ``/@user/video/<id>``.
Tải từng video: yt-dlp trước, lỗi extractor thì fallback Playwright (``tiktok_video_download``).
"""

from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path
from typing import Any, Callable, Optional

from loguru import logger
from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

StatusFn = Callable[[str], None]

_TT_USER_RE = re.compile(r"tiktok\.com/@([^/?#]+)", re.IGNORECASE)
_TT_VIDEO_RE = re.compile(
    r"(?:https?://(?:www\.)?tiktok\.com)?/@([^/?#]+)/video/(\d{8,})",
    re.IGNORECASE,
)
_TT_VIDEO_ID_RE = re.compile(r"/video/(\d{8,})")

_JS_COLLECT = """
() => {
  const found = [];
  const seen = new Set();
  const add = (user, id) => {
    const vid = String(id || "");
    if (!/^\\d{8,}$/.test(vid)) return;
    const u = String(user || "").replace(/^@/, "").split("?")[0];
    const url = u
      ? `https://www.tiktok.com/@${u}/video/${vid}`
      : `https://www.tiktok.com/video/${vid}`;
    if (seen.has(url)) return;
    seen.add(url);
    found.push(url);
  };
  document.querySelectorAll('a[href*="/video/"]').forEach((a) => {
    const h = a.getAttribute("href") || a.href || "";
    const m = String(h).match(/@([^/?#]+)\\/video\\/(\\d+)/);
    if (m) add(m[1], m[2]);
  });
  const tryParse = (txt) => {
    if (!txt) return;
    try {
      const data = JSON.parse(txt);
      const walk = (obj) => {
        if (!obj || typeof obj !== "object") return;
        if (Array.isArray(obj)) {
          obj.forEach(walk);
          return;
        }
        const id = obj.id || obj.aweme_id || obj.videoId;
        const author = (obj.author && (obj.author.uniqueId || obj.author.unique_id))
          || obj.uniqueId;
        if (id && (obj.video || obj.desc != null || obj.stats || obj.contents)) {
          add(author, id);
        }
        Object.values(obj).forEach(walk);
      };
      walk(data);
    } catch (_e) {}
  };
  const uni = document.getElementById("__UNIVERSAL_DATA_FOR_REHYDRATION__");
  if (uni) tryParse(uni.textContent);
  const sigi = document.getElementById("SIGI_STATE");
  if (sigi) tryParse(sigi.textContent);
  return found;
}
"""


def _noop_status(_: str) -> None:
    pass


def _env_headless_default() -> bool:
    raw = str(os.environ.get("TOOLFB_TT_PROFILE_HEADLESS", "1")).strip().lower()
    return raw not in {"0", "false", "no", "off"}


def normalize_tiktok_profile_url(url: str) -> str:
    """Chuẩn hoá ``https://www.tiktok.com/@user`` (bỏ query, video lẻ)."""
    raw = (url or "").strip()
    if not raw:
        return raw
    if not raw.lower().startswith("http"):
        raw = "https://" + raw.lstrip("/")
    m = _TT_USER_RE.search(raw)
    if not m:
        return (url or "").strip()
    user = m.group(1).strip()
    if not user:
        return (url or "").strip()
    return f"https://www.tiktok.com/@{user}"


def tiktok_username_from_url(url: str) -> str:
    """Lấy uniqueId từ URL profile."""
    m = _TT_USER_RE.search(url or "")
    return (m.group(1) if m else "").strip()


def extract_tiktok_video_urls(username: str, hrefs: list[str], text: str = "") -> list[str]:
    """Gom URL ``/@user/video/<id>`` từ href và HTML/JSON."""
    user = str(username or "").replace("@", "").strip()
    out: list[str] = []
    seen: set[str] = set()

    def _add(owner: str, vid: str) -> None:
        owner_clean = str(owner or user or "").replace("@", "").split("?")[0].strip()
        vid_clean = str(vid or "").strip()
        if not re.fullmatch(r"\d{8,}", vid_clean):
            return
        if not owner_clean:
            return
        full = f"https://www.tiktok.com/@{owner_clean}/video/{vid_clean}"
        if full in seen:
            return
        seen.add(full)
        out.append(full)

    for href in hrefs or []:
        s = str(href or "").strip()
        if not s:
            continue
        m = _TT_VIDEO_RE.search(s)
        if m:
            _add(m.group(1), m.group(2))
            continue
        m2 = _TT_VIDEO_ID_RE.search(s)
        if m2 and user:
            _add(user, m2.group(1))

    blob = str(text or "")
    for owner, vid in _TT_VIDEO_RE.findall(blob):
        _add(owner, vid)
    if user:
        for vid in _TT_VIDEO_ID_RE.findall(blob):
            _add(user, vid)
    return out


def extract_tiktok_video_urls_from_json(payload: object, username: str = "") -> list[str]:
    """Đi JSON TikTok (itemList / aweme) lấy link video."""
    user = str(username or "").replace("@", "").strip()
    out: list[str] = []
    seen: set[str] = set()

    def _add(owner: str, vid: str) -> None:
        owner_clean = str(owner or user or "").replace("@", "").split("?")[0].strip()
        vid_clean = str(vid or "").strip()
        if not re.fullmatch(r"\d{8,}", vid_clean) or not owner_clean:
            return
        full = f"https://www.tiktok.com/@{owner_clean}/video/{vid_clean}"
        if full in seen:
            return
        seen.add(full)
        out.append(full)

    def _walk(obj: object) -> None:
        if obj is None:
            return
        if isinstance(obj, list):
            for item in obj:
                _walk(item)
            return
        if not isinstance(obj, dict):
            return
        vid = str(obj.get("id") or obj.get("aweme_id") or obj.get("videoId") or "").strip()
        author = obj.get("author")
        owner = ""
        if isinstance(author, dict):
            owner = str(author.get("uniqueId") or author.get("unique_id") or "").strip()
        if not owner:
            owner = str(obj.get("uniqueId") or "").strip()
        looks_video = any(k in obj for k in ("video", "desc", "stats", "contents", "aweme_id"))
        if vid and looks_video:
            _add(owner, vid)
        for value in obj.values():
            if isinstance(value, (dict, list)):
                _walk(value)

    _walk(payload)
    return out


def _resolve_cookie_storage_state(cookie_path: str | None) -> str | None:
    p = str(cookie_path or "").strip()
    if not p:
        return None
    path = Path(p).expanduser()
    if not path.is_file():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if isinstance(raw, dict) and ("cookies" in raw or "origins" in raw):
        return str(path.resolve())
    return None


def tiktok_ytdlp_profile_broken(err: str) -> bool:
    """yt-dlp gãy extractor kênh — nên quét bằng trình duyệt."""
    text = str(err or "").lower()
    if not text:
        return False
    keys = (
        "secondary user id",
        "unable to extract",
        "unable to download webpage",
        "http error 4",
        "this account is private",
        "the extractor is not currently",
        "no video formats",
        "unsupported url",
    )
    return any(key in text for key in keys)


def scan_tiktok_profile_page(
    *,
    page_url: str,
    cookie_path: str | None = None,
    max_videos: int = 50,
    max_scroll_rounds: int = 40,
    max_scan_minutes: float = 8.0,
    scroll_pause_sec: float = 1.0,
    headless: bool | None = None,
    status: StatusFn | None = None,
    on_partial: Optional[Callable[[list[str]], None]] = None,
) -> dict[str, Any]:
    """
    Mở profile TikTok, cuộn và trả URL từng video.

    Returns:
        ``{ "ok": bool, "items": [{"video_id", "url", "title"}], "message": str }``
    """
    st = status or _noop_status
    url = normalize_tiktok_profile_url(page_url.strip())
    user = tiktok_username_from_url(url)
    if "tiktok.com" not in url.lower() or not user:
        return {"ok": False, "items": [], "message": "URL không phải profile TikTok."}

    hl = _env_headless_default() if headless is None else bool(headless)
    max_videos = max(1, min(5_000, int(max_videos)))
    # Danh sách dài cần nhiều vòng cuộn hơn mức gọi cũ (tối đa 80).
    need_rounds = max(4, int(max_videos) // 2 + 12)
    max_scroll_rounds = max(max(4, int(max_scroll_rounds)), min(360, need_rounds))
    base_minutes = float(max_scan_minutes or 8.0)
    if max_videos > 80:
        base_minutes = max(base_minutes, min(40.0, 8.0 + max_videos / 25.0))
    max_scan_minutes = max(1.0, min(45.0, base_minutes))
    started_at = time.monotonic()
    hard_deadline = started_at + (max_scan_minutes * 60.0)

    ordered_urls: list[str] = []
    seen_urls: set[str] = set()

    def _add_url(u: str) -> None:
        s = str(u or "").strip()
        if not s or s in seen_urls or len(seen_urls) >= max_videos:
            return
        seen_urls.add(s)
        ordered_urls.append(s)

    def _add_many(urls: list[str]) -> None:
        for item in urls:
            _add_url(item)

    def _emit_partial() -> None:
        if not on_partial:
            return
        try:
            on_partial(list(ordered_urls))
        except Exception as exc:  # noqa: BLE001
            logger.debug("tiktok on_partial: {}", exc)

    def _ingest_payload(payload: object) -> None:
        _add_many(extract_tiktok_video_urls_from_json(payload, user))

    def _collect_from_page(page: Any) -> None:
        try:
            js_urls = page.evaluate(_JS_COLLECT)
        except Exception:
            js_urls = []
        if isinstance(js_urls, list):
            _add_many([str(x) for x in js_urls])
        try:
            hrefs = page.eval_on_selector_all(
                "a[href]",
                "els => els.map(a => a.getAttribute('href') || a.href || '')",
            )
        except Exception:
            hrefs = []
        _add_many(extract_tiktok_video_urls(user, [str(h) for h in hrefs]))

    def _on_response(resp: Any) -> None:
        try:
            u = str(getattr(resp, "url", "") or "").lower()
        except Exception:
            return
        if not any(key in u for key in ("item_list", "item/list", "aweme/post", "aweme/v1/web")):
            return
        try:
            data = resp.json()
        except Exception:
            return
        _ingest_payload(data)
        _emit_partial()

    def _is_login_wall(url_now: str, body_text: str) -> bool:
        u = str(url_now or "").lower()
        b = str(body_text or "").lower()
        if "/login" in u and "tiktok.com" in u:
            return True
        return any(
            x in b
            for x in (
                "log in to tiktok",
                "đăng nhập vào tiktok",
                "đăng nhập để xem",
            )
        )

    storage = _resolve_cookie_storage_state(cookie_path)

    try:
        with sync_playwright() as pw:
            from src.utils.dom_input import chromium_background_launch_kwargs

            browser = pw.chromium.launch(**chromium_background_launch_kwargs(headless=hl))
            try:
                ctx_kwargs: dict[str, Any] = {
                    "viewport": {"width": 1280, "height": 900},
                    "locale": "vi-VN",
                    "user_agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
                    ),
                }
                if storage:
                    ctx_kwargs["storage_state"] = storage
                    st("Đang dùng cookie Playwright từ config.")
                context = browser.new_context(**ctx_kwargs)
                page = context.new_page()
                page.on("response", _on_response)
                try:
                    Stealth().apply_stealth_sync(page)
                except Exception:
                    pass
                st(f"Đang mở profile TikTok @{user} bằng trình duyệt…")
                page.goto(url, wait_until="domcontentloaded", timeout=120_000)
                time.sleep(min(2.2, scroll_pause_sec + 0.8))
                try:
                    body_text = page.inner_text("body", timeout=4000)
                except Exception:
                    body_text = ""
                if _is_login_wall(page.url, body_text):
                    return {
                        "ok": False,
                        "items": [],
                        "message": (
                            "TikTok yêu cầu đăng nhập. Thử lại sau hoặc dùng cookie Playwright "
                            "(storage_state JSON) nếu kênh riêng tư."
                        ),
                    }
                _collect_from_page(page)
                _emit_partial()

                stable = 0
                prev_count = 0
                for i in range(max_scroll_rounds):
                    if time.monotonic() >= hard_deadline:
                        st(f"Đã chạm giới hạn {max_scan_minutes:.0f} phút — dừng.")
                        break
                    if len(ordered_urls) >= max_videos:
                        break
                    st(f"Cuộn {i + 1}/{max_scroll_rounds} — đã thấy {len(ordered_urls)} video TikTok…")
                    try:
                        page.evaluate(
                            "() => {"
                            " const e=document.scrollingElement||document.documentElement;"
                            " const step=Math.floor((window.innerHeight||800)*0.95);"
                            " e.scrollBy(0, step);"
                            " const nodes=document.querySelectorAll('div');"
                            " let n=0;"
                            " for (const d of nodes) {"
                            "   if (n>=6) break;"
                            "   if (d.scrollHeight > d.clientHeight + 400) { d.scrollBy(0, step); n++; }"
                            " }"
                            "}"
                        )
                    except Exception:
                        pass
                    time.sleep(scroll_pause_sec)
                    _collect_from_page(page)
                    _emit_partial()
                    if len(ordered_urls) == prev_count:
                        stable += 1
                        # TikTok ảo hóa lưới: vài lần không thêm link chưa phải hết kênh.
                        if stable >= 22:
                            break
                        if stable in (8, 16):
                            try:
                                page.evaluate(
                                    "() => { const e=document.scrollingElement||document.documentElement;"
                                    " e.scrollTop = e.scrollHeight; }"
                                )
                            except Exception:
                                pass
                    else:
                        stable = 0
                    prev_count = len(ordered_urls)
                context.close()
            finally:
                browser.close()
    except Exception as exc:  # noqa: BLE001
        logger.exception("scan_tiktok_profile_page")
        return {"ok": False, "items": [], "message": str(exc)}

    elapsed_sec = int(max(0.0, time.monotonic() - started_at))
    items = [
        {
            "video_id": (u.rsplit("/", 1)[-1] if "/" in u else str(i + 1)),
            "url": u,
            "title": f"TikTok {i + 1}",
        }
        for i, u in enumerate(ordered_urls)
    ]
    if not items:
        return {
            "ok": False,
            "items": [],
            "message": (
                "Không thu được video nào trên profile. Kênh có thể trống, riêng tư, "
                "hoặc TikTok chặn trang. Thử dán từng link /@user/video/… rồi tải."
            ),
        }
    return {
        "ok": True,
        "items": items,
        "message": f"Đã quét {len(items)} video TikTok bằng trình duyệt ({elapsed_sec}s).",
    }
