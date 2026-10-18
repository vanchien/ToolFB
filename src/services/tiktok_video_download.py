"""
Tải video TikTok lẻ khi yt-dlp gãy extractor.

yt-dlp thường báo ``Unexpected response from webpage request`` / thiếu impersonate.
Mở trang bằng Playwright, lấy ``downloadAddr`` / ``playAddr`` / URL mạng rồi tải file.
Giữ một phiên Chromium để tải hàng loạt (hàng trăm URL) không mở browser mỗi lần.
"""

from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import urlparse

from loguru import logger
from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

StatusFn = Callable[[str], None]

_TT_VIDEO_RE = re.compile(
    r"tiktok\.com/@([^/?#]+)/video/(\d+)",
    re.IGNORECASE,
)
_TT_ID_ONLY_RE = re.compile(r"/video/(\d+)", re.IGNORECASE)
_TT_SHORT_HOSTS = ("vm.tiktok.com", "vt.tiktok.com")

_DOWNLOAD_KEYS = frozenset({"downloadaddr", "download_addr", "downloadurl", "download_url"})
_PLAY_KEYS = frozenset({"playaddr", "play_addr", "playurl", "play_url"})
_URL_LIST_KEYS = frozenset({"urllist", "url_list", "urllistv2"})

_JS_EXTRACT = """
() => {
  const download = [];
  const play = [];
  const titleBits = [];
  const seen = new Set();
  const add = (bucket, raw) => {
    const u = String(raw || "").trim();
    if (!u || !/^https?:\\/\\//i.test(u) || seen.has(u)) return;
    if (u.startsWith("blob:") || u.startsWith("data:")) return;
    seen.add(u);
    bucket.push(u);
  };
  const walk = (obj, hint) => {
    if (!obj || typeof obj !== "object") return;
    if (Array.isArray(obj)) {
      obj.forEach((x) => walk(x, hint));
      return;
    }
    const desc = obj.desc || obj.title || obj.nickname;
    if (typeof desc === "string" && desc.trim() && titleBits.length < 4) {
      titleBits.push(desc.trim());
    }
    for (const [k, v] of Object.entries(obj)) {
      const key = String(k || "").toLowerCase();
      const nextHint = (
        ["downloadaddr", "download_addr", "downloadurl", "download_url"].includes(key)
          ? "dl"
          : (["playaddr", "play_addr", "playurl", "play_url"].includes(key) ? "play" : hint)
      );
      if (typeof v === "string") {
        if (nextHint === "dl") add(download, v);
        else if (nextHint === "play" || /\\.mp4(\\?|$)/i.test(v) || v.includes("/video/tos/")) add(play, v);
      } else if (v && typeof v === "object") {
        if (typeof v.Url === "string") add(nextHint === "dl" ? download : play, v.Url);
        if (typeof v.url === "string") add(nextHint === "dl" ? download : play, v.url);
        const lists = v.UrlList || v.url_list || v.UrlListV2;
        if (Array.isArray(lists)) {
          lists.forEach((x) => add(nextHint === "dl" ? download : play, x));
        }
        walk(v, nextHint);
      }
    }
  };
  const uni = document.getElementById("__UNIVERSAL_DATA_FOR_REHYDRATION__");
  if (uni) {
    try { walk(JSON.parse(uni.textContent || "{}"), ""); } catch (_e) {}
  }
  const sigi = document.getElementById("SIGI_STATE");
  if (sigi) {
    try { walk(JSON.parse(sigi.textContent || "{}"), ""); } catch (_e) {}
  }
  document.querySelectorAll("video").forEach((v) => {
    add(play, v.currentSrc || v.src);
    v.querySelectorAll("source").forEach((s) => add(play, s.src));
  });
  return { download, play, title: titleBits[0] || "" };
}
"""

_CHROME_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)


def _noop_status(_: str) -> None:
    pass


def _env_headless_default() -> bool:
    raw = str(os.environ.get("TOOLFB_TT_PROFILE_HEADLESS", "1")).strip().lower()
    return raw not in {"0", "false", "no", "off"}


def is_tiktok_video_url(url: str) -> bool:
    """True nếu URL là video TikTok (trang lẻ hoặc link rút gọn)."""
    raw = str(url or "").strip()
    if not raw:
        return False
    u = raw.lower()
    if "tiktok.com" not in u:
        return False
    if "/video/" in u:
        return True
    host = (urlparse(raw).hostname or "").lower()
    if host in _TT_SHORT_HOSTS:
        return True
    if "/t/" in u and "tiktok.com" in u:
        return True
    return False


def parse_tiktok_video_ref(url: str) -> tuple[str, str]:
    """Trả ``(username, video_id)`` — có thể rỗng nếu link rút gọn."""
    raw = str(url or "").strip()
    m = _TT_VIDEO_RE.search(raw)
    if m:
        return m.group(1).strip(), m.group(2).strip()
    m2 = _TT_ID_ONLY_RE.search(raw)
    if m2:
        return "", m2.group(1).strip()
    return "", ""


def tiktok_ytdlp_video_broken(err: str) -> bool:
    """yt-dlp gãy extractor video lẻ — nên tải bằng trình duyệt."""
    text = str(err or "").lower()
    if not text:
        return False
    keys = (
        "unexpected response from webpage request",
        "no impersonate target",
        "impersonate target is available",
        "unable to extract",
        "unable to download webpage",
        "http error 4",
        "no video formats",
        "the extractor is not currently",
        "fresh cookies are needed",
        "this video is unavailable",
        "unsupported url",
        "ip address is blocked",
        "blocked from accessing",
        "geo-restricted",
        "not available in your country",
        "this content is not available",
        "webpage is protected",
        "empty media response",
    )
    return any(key in text for key in keys)


def _looks_like_media(data: bytes) -> bool:
    if not data or len(data) < 32:
        return False
    if data[4:8] == b"ftyp":
        return True
    if data[:4] == b"\x1aE\xdf\xa3":
        return True
    if data[:4] == b"RIFF" and data[8:12] == b"AVI ":
        return True
    if data[:3] == b"ID3" or data[:2] in (b"\xff\xfb", b"\xff\xf3"):
        return False
    return False


def _safe_filename(text: str, fallback: str) -> str:
    raw = re.sub(r"[\\/:*?\"<>|\r\n\t]+", " ", str(text or "")).strip()
    raw = re.sub(r"\s+", " ", raw)[:80].strip(" .")
    return raw or fallback


def build_tiktok_dest_path(
    *,
    output_dir: str,
    url: str,
    title: str = "",
    organize_by_platform: bool = True,
    organize_by_uploader: bool = True,
) -> Path:
    """Đường dẫn file theo cùng quy ước thư mục job tải."""
    user, vid = parse_tiktok_video_ref(url)
    uploader = user or "UnknownUploader"
    vid = vid or "unknown"
    stem = _safe_filename(title, f"tiktok_{vid}")
    name = f"unknown_date_{vid}_{stem}.mp4"
    root = Path(str(output_dir or ".")).expanduser()
    if organize_by_platform and organize_by_uploader:
        return root / "TikTok" / uploader / name
    if organize_by_platform:
        return root / "TikTok" / name
    return root / name


def extract_tiktok_media_urls(payload: object) -> dict[str, Any]:
    """
    Đi JSON TikTok lấy URL media + tiêu đề.

    Returns:
        ``{"download": [...], "play": [...], "title": str, "photo": bool}``
    """
    download: list[str] = []
    play: list[str] = []
    seen: set[str] = set()
    title = ""
    photo = False

    def _add(bucket: list[str], raw: object) -> None:
        u = str(raw or "").strip()
        if not u.startswith("http") or u in seen:
            return
        if u.startswith("blob:") or u.startswith("data:"):
            return
        seen.add(u)
        bucket.append(u)

    def _ingest_addr(node: object, bucket: list[str]) -> None:
        if isinstance(node, str):
            _add(bucket, node)
            return
        if not isinstance(node, dict):
            return
        for key in ("url", "Url", "uri", "Uri"):
            if node.get(key):
                _add(bucket, node.get(key))
        for key in ("url_list", "UrlList", "UrlListV2"):
            vals = node.get(key)
            if isinstance(vals, list):
                for item in vals:
                    _add(bucket, item)

    def _walk(obj: object, hint: str = "") -> None:
        nonlocal title, photo
        if obj is None:
            return
        if isinstance(obj, list):
            for item in obj:
                _walk(item, hint)
            return
        if not isinstance(obj, dict):
            return
        if not title:
            for key in ("desc", "title", "nickname"):
                val = str(obj.get(key) or "").strip()
                if val:
                    title = val
                    break
        if any(k in obj for k in ("imagePost", "image_post", "imagePostInfo")):
            photo = True
        for key, value in obj.items():
            low = str(key or "").lower()
            next_hint = hint
            if low in _DOWNLOAD_KEYS:
                next_hint = "dl"
                _ingest_addr(value, download)
            elif low in _PLAY_KEYS:
                next_hint = "play"
                _ingest_addr(value, play)
            elif low in _URL_LIST_KEYS and hint in {"dl", "play"}:
                if isinstance(value, list):
                    for item in value:
                        _add(download if hint == "dl" else play, item)
            elif isinstance(value, str) and (
                ".mp4" in value.lower() or "/video/tos/" in value.lower()
            ):
                _add(play if hint != "dl" else download, value)
            if isinstance(value, (dict, list)):
                _walk(value, next_hint)

    _walk(payload)
    return {"download": download, "play": play, "title": title, "photo": photo}


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


def _network_looks_like_video(url: str, content_type: str) -> bool:
    u = str(url or "").lower()
    ct = str(content_type or "").lower()
    if "m3u8" in u or "mpegurl" in ct:
        return False
    if "video/" in ct or "mp4" in ct:
        return True
    return any(
        key in u
        for key in (
            ".mp4",
            "/video/tos/",
            "mime_type=video",
            "video_mp4",
            "aweme/v1/play",
        )
    )


def _write_sidecar_info(dest: Path, *, url: str, title: str, uploader: str, video_id: str) -> None:
    info = {
        "id": video_id,
        "title": title or dest.stem,
        "uploader": uploader,
        "webpage_url": url,
        "original_url": url,
        "extractor": "tiktok_playwright",
        "ext": "mp4",
    }
    try:
        dest.with_suffix(".info.json").write_text(
            json.dumps(info, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    except OSError as exc:
        logger.debug("tiktok info.json: {}", exc)


class TikTokBrowserDownloader:
    """Một phiên Chromium dùng lại cho nhiều video TikTok."""

    def __init__(
        self,
        *,
        cookie_path: str | None = None,
        headless: bool | None = None,
        status: StatusFn | None = None,
        cancel_check: Optional[Callable[[], bool]] = None,
    ) -> None:
        self._cookie_path = cookie_path
        self._headless = _env_headless_default() if headless is None else bool(headless)
        self._status = status or _noop_status
        self._cancel_check = cancel_check or (lambda: False)
        self._pw: Any = None
        self._browser: Any = None
        self._context: Any = None
        self._started = False
        self._start_error = ""
        self._batch_uses = 0

    def start(self) -> None:
        """Mở Chromium một lần. Gọi lại nếu đã mở thì bỏ qua."""
        if self._started and self._context is not None:
            return
        if self._start_error:
            raise RuntimeError(self._start_error)
        storage = _resolve_cookie_storage_state(self._cookie_path)
        try:
            self._pw = sync_playwright().start()
            from src.utils.dom_input import chromium_background_launch_kwargs

            self._browser = self._pw.chromium.launch(
                **chromium_background_launch_kwargs(headless=self._headless)
            )
            ctx_kwargs: dict[str, Any] = {
                "viewport": {"width": 1280, "height": 900},
                "locale": "vi-VN",
                "user_agent": _CHROME_UA,
            }
            if storage:
                ctx_kwargs["storage_state"] = storage
                self._status("Đang dùng cookie Playwright từ config.")
            self._context = self._browser.new_context(**ctx_kwargs)
            self._started = True
        except Exception as exc:  # noqa: BLE001
            self._start_error = f"Không mở được trình duyệt để tải TikTok: {exc}"
            self.close()
            raise RuntimeError(self._start_error) from exc

    def close(self) -> None:
        """Đóng browser + Playwright (an toàn khi gọi nhiều lần)."""
        for obj, meth in (
            (self._context, "close"),
            (self._browser, "close"),
            (self._pw, "stop"),
        ):
            if obj is None:
                continue
            try:
                getattr(obj, meth)()
            except Exception:
                pass
        self._context = None
        self._browser = None
        self._pw = None
        self._started = False
        self._batch_uses = 0

    def healthy(self) -> bool:
        """Phiên còn mở và Chromium chưa bị đóng/crash."""
        if not self._started or self._context is None or self._browser is None:
            return False
        try:
            return bool(self._browser.is_connected())
        except Exception:
            return False

    def download(self, page_url: str, dest: Path) -> dict[str, Any]:
        """
        Mở 1 URL video và ghi file ``dest``.

        Returns:
            ``{"ok": bool, "path": str, "title": str, "message": str}``
        """
        url = str(page_url or "").strip()
        if not url:
            return {"ok": False, "path": "", "title": "", "message": "Thiếu URL TikTok."}
        if self._cancel_check():
            return {"ok": False, "path": "", "title": "", "message": "Đã hủy/tạm dừng bởi người dùng."}
        self.start()
        if dest.is_file() and dest.stat().st_size >= 20_000:
            return {
                "ok": True,
                "path": str(dest.resolve()),
                "title": dest.stem,
                "message": "File đã có sẵn — bỏ qua tải lại.",
            }

        captured: list[str] = []
        seen_net: set[str] = set()

        def _on_response(resp: Any) -> None:
            try:
                u = str(getattr(resp, "url", "") or "")
                headers = getattr(resp, "headers", {}) or {}
                ct = ""
                if isinstance(headers, dict):
                    ct = str(headers.get("content-type") or headers.get("Content-Type") or "")
            except Exception:
                return
            if not _network_looks_like_video(u, ct):
                return
            if u in seen_net:
                return
            seen_net.add(u)
            captured.append(u)

        page = None
        title = ""
        photo = False
        try:
            page = self._context.new_page()
            try:
                Stealth().apply_stealth_sync(page)
            except Exception:
                pass
            page.on("response", _on_response)
            user, vid = parse_tiktok_video_ref(url)
            self._status(f"Đang mở video TikTok {vid or url[-18:]} bằng trình duyệt…")
            page.goto(url, wait_until="domcontentloaded", timeout=90_000)
            time.sleep(1.1)
            try:
                body_text = page.inner_text("body", timeout=4000)
            except Exception:
                body_text = ""
            if _is_login_wall(page.url, body_text):
                return {
                    "ok": False,
                    "path": "",
                    "title": "",
                    "message": (
                        "TikTok yêu cầu đăng nhập. Dùng cookie Playwright (storage_state JSON) "
                        "nếu video riêng tư."
                    ),
                }
            try:
                page.wait_for_selector(
                    "video, #__UNIVERSAL_DATA_FOR_REHYDRATION__, #SIGI_STATE",
                    timeout=12_000,
                )
            except Exception:
                pass
            try:
                page.evaluate(
                    "() => { const v = document.querySelector('video');"
                    " if (v) { v.muted = true; v.play().catch(() => {}); } }"
                )
            except Exception:
                pass
            time.sleep(1.4)
            js_data: dict[str, Any] = {}
            try:
                raw_js = page.evaluate(_JS_EXTRACT)
                if isinstance(raw_js, dict):
                    js_data = raw_js
            except Exception as exc:  # noqa: BLE001
                logger.debug("tiktok JS extract: {}", exc)
            title = str(js_data.get("title") or "").strip()
            download_urls = [str(x) for x in (js_data.get("download") or []) if str(x).strip()]
            play_urls = [str(x) for x in (js_data.get("play") or []) if str(x).strip()]
            for script_id in ("__UNIVERSAL_DATA_FOR_REHYDRATION__", "SIGI_STATE"):
                try:
                    txt = page.evaluate(
                        "(id) => { const e = document.getElementById(id); return e ? e.textContent : ''; }",
                        script_id,
                    )
                    if txt:
                        parsed = extract_tiktok_media_urls(json.loads(txt))
                        download_urls.extend(parsed.get("download") or [])
                        play_urls.extend(parsed.get("play") or [])
                        title = title or str(parsed.get("title") or "")
                        photo = photo or bool(parsed.get("photo"))
                except Exception:
                    pass
            if not user:
                user, vid = parse_tiktok_video_ref(str(page.url or url))
            candidates = _dedupe_urls([*download_urls, *play_urls, *captured])
            if not candidates:
                if photo:
                    return {
                        "ok": False,
                        "path": "",
                        "title": title,
                        "message": "Đây là bài ảnh (photo mode) TikTok — không có file video.",
                    }
                return {
                    "ok": False,
                    "path": "",
                    "title": title,
                    "message": "Không tìm thấy URL file video trên trang TikTok.",
                }
            dest.parent.mkdir(parents=True, exist_ok=True)
            last_err = ""
            for media_url in candidates[:8]:
                if self._cancel_check():
                    return {
                        "ok": False,
                        "path": "",
                        "title": title,
                        "message": "Đã hủy/tạm dừng bởi người dùng.",
                    }
                ok, last_err = self._fetch_media(media_url, dest)
                if ok:
                    _write_sidecar_info(
                        dest,
                        url=url,
                        title=title,
                        uploader=user,
                        video_id=vid,
                    )
                    return {
                        "ok": True,
                        "path": str(dest.resolve()),
                        "title": title or dest.stem,
                        "message": "Đã tải video TikTok bằng trình duyệt.",
                    }
            return {
                "ok": False,
                "path": "",
                "title": title,
                "message": last_err or "Không tải được stream video TikTok.",
            }
        except Exception as exc:  # noqa: BLE001
            logger.exception("TikTokBrowserDownloader.download")
            return {"ok": False, "path": "", "title": title, "message": str(exc)}
        finally:
            if page is not None:
                try:
                    page.close()
                except Exception:
                    pass

    def _fetch_media(self, media_url: str, dest: Path) -> tuple[bool, str]:
        """Tải 1 URL stream vào ``dest``. Trả ``(ok, error)``."""
        tmp = dest.with_suffix(dest.suffix + ".part")
        try:
            resp = self._context.request.get(
                media_url,
                timeout=180_000,
                headers={
                    "Referer": "https://www.tiktok.com/",
                    "Origin": "https://www.tiktok.com",
                    "User-Agent": _CHROME_UA,
                },
            )
            status = int(getattr(resp, "status", 0) or 0)
            if status not in (200, 206):
                return False, f"HTTP {status} khi tải stream."
            body = resp.body()
            if not _looks_like_media(body) and len(body) < 80_000:
                return False, "Phản hồi không phải file video."
            if len(body) < 20_000:
                return False, f"File quá nhỏ ({len(body)} byte) — bỏ qua."
            tmp.write_bytes(body)
            tmp.replace(dest)
            return True, ""
        except Exception as exc:  # noqa: BLE001
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass
            return False, str(exc)


def _dedupe_urls(urls: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in urls:
        u = str(raw or "").strip()
        if not u or u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def download_tiktok_video_via_browser(
    *,
    page_url: str,
    dest: Path,
    cookie_path: str | None = None,
    headless: bool | None = None,
    status: StatusFn | None = None,
    cancel_check: Optional[Callable[[], bool]] = None,
    session: TikTokBrowserDownloader | None = None,
) -> dict[str, Any]:
    """
    Tải 1 video TikTok. Nếu truyền ``session`` thì tái sử dụng browser đã mở.

    Returns:
        ``{"ok": bool, "path": str, "title": str, "message": str}``
    """
    own = session is None
    dl = session or TikTokBrowserDownloader(
        cookie_path=cookie_path,
        headless=headless,
        status=status,
        cancel_check=cancel_check,
    )
    try:
        return dl.download(page_url, dest)
    finally:
        if own:
            dl.close()
