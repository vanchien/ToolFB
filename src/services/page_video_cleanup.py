"""
Quét và xóa video/reel đã đăng trên **đúng Page đang chọn** (Playwright, không Graph API).

Một phiên trình duyệt: chuyển vai trò Page → cuộn tab Reels/Videos của Page đó
→ lọc link thuộc Page → xóa video đã chọn (không mở lại browser).
"""

from __future__ import annotations

import base64
import io
import queue
import random
import re
import threading
import time
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

from loguru import logger

from src.services.facebook_groups.source_watch import video_id_from_url
from src.services.facebook_reels_catalog import (
    _JS_SCROLL_DEEP,
    _JS_SCROLL_SNAP,
    _collect_main_hrefs,
    _profile_videos_tab_url,
    facebook_page_identity,
    normalize_facebook_reels_tab_url,
    url_matches_facebook_page,
)
from src.services.page_video_harvest import (
    NetworkVideoHarvest,
    is_real_facebook_video_id,
    merge_video_item_lists,
    parse_facebook_publish_stamp,
    sort_videos_by_published,
)
from src.utils.dom_input import click_locator_dom

StatusFn = Callable[[str], None]

_MORE_NAME = re.compile(
    r"More|Menu|See more|Actions for this post|More options|"
    r"Hành động khác|Tùy chọn khác|Thêm tùy chọn|^Thêm$",
    re.I,
)
_DELETE_ITEM = re.compile(
    r"Move (?:this )?(?:post |reel |video )?to (?:bin|trash|recycle(?:\s+bin)?)|"
    r"Delete (?:this )?(?:reel|video|post)|"
    r"Remove (?:this )?(?:reel|video|post)|"
    r"Xóa(?:\s+(?:video|bài(?:\s*viết)?|bài đăng|Reel|reel))|"
    r"Chuyển vào thùng rác",
    re.I,
)
_CONFIRM_BTN = re.compile(
    r"^(?:Delete|Move|OK|Xóa|Chuyển|Move to bin|Xác nhận|Yes)$",
    re.I,
)
_SUCCESS_HINT = re.compile(
    r"moved to (?:bin|trash)|đã chuyển vào thùng rác|đã xóa|deleted|moved to recycle",
    re.I,
)


def _noop(msg: str) -> None:
    del msg


def resolve_page_cleanup_target(page_row: dict[str, Any] | None) -> dict[str, str]:
    """Lấy URL / tên / Meta ID của Page đang chọn — dùng xuyên suốt quét và xóa."""
    row = dict(page_row or {})
    url = str(row.get("page_url") or "").strip()
    name = str(row.get("page_name") or "").strip()
    fb_id = str(row.get("fb_page_id") or row.get("facebook_page_id") or "").strip()
    if not fb_id.isdigit():
        ident = facebook_page_identity(url)
        if ident.startswith("id:"):
            fb_id = ident.split(":", 1)[1]
        else:
            m = re.search(r"facebook\.com/(?:profile\.php\?id=)?(\d{8,})", url, re.I)
            fb_id = m.group(1) if m else ""
    ident = facebook_page_identity(url)
    return {
        "page_url": url,
        "page_name": name,
        "fb_page_id": fb_id,
        "account_id": str(row.get("account_id") or "").strip(),
        "identity": ident,
    }


def classify_page_video_url(url: str) -> dict[str, str]:
    """Chuẩn hoá một link video/reel Page thành ``video_id`` / ``kind`` / ``url``."""
    raw = str(url or "").strip()
    vid = video_id_from_url(raw)
    low = raw.casefold()
    if "/reel/" in low or "sk=reels" in low:
        kind = "Reel"
        canon = f"https://www.facebook.com/reel/{vid}" if vid else raw
    else:
        kind = "Video"
        canon = raw
    return {"video_id": vid, "kind": kind, "url": canon or raw}


def _abs_facebook_href(href: str) -> str:
    s = str(href or "").strip()
    if not s:
        return ""
    if s.startswith("http://") or s.startswith("https://"):
        return s
    if s.startswith("/"):
        return "https://www.facebook.com" + s
    return s


def href_belongs_to_page(
    href: str,
    *,
    expected_page_url: str,
    live_page_url: str = "",
    fb_page_id: str = "",
    allow_bare_reel: bool = False,
) -> bool:
    """
    True nếu link video thuộc Page đang chọn — loại Reels toàn cục / Page khác / gợi ý.
    """
    h = _abs_facebook_href(href)
    if not h or "facebook.com" not in h.casefold():
        return False
    low = h.casefold()
    if re.search(r"facebook\.com/reels/?(\?|$)", low) and "/reel/" not in low:
        return False
    ident = facebook_page_identity(h)
    want = facebook_page_identity(expected_page_url)
    live_ident = facebook_page_identity(live_page_url) if live_page_url else ""
    fb = str(fb_page_id or "").strip()
    if ident:
        if want and ident == want:
            return True
        if live_ident and ident == live_ident:
            return True
        if fb and ident == f"id:{fb}":
            return True
        if fb and fb in h:
            return True
        return False
    if allow_bare_reel and video_id_from_url(h):
        return True
    return False


def normalize_scanned_video_items(raw_items: list[Any]) -> list[dict[str, str]]:
    """Gộp list quét thành bản ghi có ID thật — bỏ trùng."""
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in raw_items or []:
        if isinstance(item, dict):
            url = str(item.get("url") or "").strip()
        else:
            url = str(item or "").strip()
        if not url:
            continue
        rec = classify_page_video_url(url)
        key = rec["video_id"] or rec["url"].casefold()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(rec)
    return out


def filter_page_videos(items: list[dict[str, str]], query: str) -> list[dict[str, str]]:
    """Lọc theo ID, loại hoặc đoạn URL (không phân biệt hoa thường)."""
    q = str(query or "").strip().casefold()
    if not q:
        return list(items)
    return [
        rec
        for rec in items
        if q
        in (
            f"{rec.get('video_id', '')} {rec.get('kind', '')} {rec.get('url', '')} "
            f"{rec.get('title', '')} {rec.get('published', '')} {rec.get('published_ts', '')}"
        ).casefold()
    ]


def browser_on_selected_page(
    page: Any,
    expected_page_url: str,
    *,
    fb_page_id: str = "",
    page_name: str = "",
) -> bool:
    """True nếu tab đang đứng đúng Page đã chọn (id số, vanity, hoặc Meta ID trong HTML)."""
    cur = str(getattr(page, "url", "") or "")
    if url_matches_facebook_page(cur, expected_page_url):
        return True
    try:
        from src.automation.facebook_actions import _urls_refer_same_facebook_page

        if _urls_refer_same_facebook_page(expected_page_url, cur, page=page):
            return True
    except Exception:
        pass
    fb = str(fb_page_id or "").strip()
    if fb and len(fb) >= 8:
        blob = cur
        try:
            blob += " " + str(page.content() or "")[:120_000]
        except Exception:
            pass
        if fb in blob:
            return True
    name = str(page_name or "").strip()
    if name and len(name) >= 3:
        try:
            title = str(page.title() or "")
        except Exception:
            title = ""
        if name.casefold() in title.casefold():
            return True
    return False


def _human_pause(lo_ms: int = 700, hi_ms: int = 1_600) -> None:
    time.sleep(random.uniform(lo_ms / 1000.0, hi_ms / 1000.0))


def _click_role(page: Any, role: str, pattern: re.Pattern[str], *, prefer_last: bool = True) -> bool:
    """Bấm phần tử role+tên (aria) bằng DOM click — không dùng chuột OS."""
    try:
        loc = page.get_by_role(role, name=pattern)
        n = int(loc.count())
    except Exception:
        return False
    if n <= 0:
        return False
    order = range(n - 1, -1, -1) if prefer_last else range(n)
    for i in order:
        item = loc.nth(i)
        try:
            if not item.is_visible(timeout=350):
                continue
        except Exception:
            continue
        if click_locator_dom(item, timeout_ms=2_400):
            return True
    return False


def _page_hint_text(page: Any) -> str:
    try:
        return str(page.inner_text("body", timeout=1_500) or "")
    except Exception:
        return ""


_CONTENT_LIBRARY = "https://www.facebook.com/professional_dashboard/content/content_library/"
_DURATION_RX = re.compile(r"\b(?:\d+h\s*)?(?:\d+m\s*)?\d+s\b|\b\d{1,2}:\d{2}\b", re.I)
_PUBLISHED_RX = re.compile(
    r"(?:Today|Yesterday|Hôm nay|Hôm qua)[^\n]{0,40}"
    r"|(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2}(?:,\s+\d{4})?[^\n]{0,24}"
    r"|\d{1,2}/\d{1,2}/\d{2,4}",
    re.I,
)
_DATE_RANGE_BTN = re.compile(
    r"Last\s+\d+\s+days|past\s+\d+\s+days|lifetime|all time|maximum|"
    r"\d+\s*ngày(?:\s+trước)?|toàn thời gian",
    re.I,
)
_ALL_TIME_ITEM = re.compile(
    r"all time|lifetime|maximum|ever|toàn thời gian|tất cả thời gian|"
    r"mọi lúc|từ trước|toàn bộ|last 365|365 days|2 years|10 years",
    re.I,
)
_CUSTOM_RANGE = re.compile(r"^Custom$|^Custom range$|^Tùy chỉnh$|^Khoảng tùy chỉnh$", re.I)
_APPLY_RANGE = re.compile(r"^Apply$|^Update$|^Done$|^Save$|^Áp dụng$|^Cập nhật$|^Xong$", re.I)
_PUBLISHED_TAB = re.compile(r"^Published$|^Đã đăng$|^Đã xuất bản$", re.I)
_LIBRARY_NAV = re.compile(r"Content Library|Thư viện nội dung", re.I)
_DELETE_POSTS = re.compile(
    r"Delete posts|Delete post|Xóa bài viết|Xóa bài đăng|Xóa các bài|"
    r"Move (?:posts|post)?\s*to (?:bin|trash)|Chuyển vào thùng rác",
    re.I,
)
_SELECT_ALL = re.compile(r"select all|chọn tất cả|chọn mọi", re.I)
_HEADER_ROW = re.compile(
    r"\b(title|views|reach|published|engagement|impressions|comments)\b",
    re.I,
)
_TITLE_NOISE = re.compile(
    r"^[\d$,.\s:%\-—_+x]+$|"
    r"^(published|views?|reach|impressions?|engagement|reel|reels|video|videos|photo(?:\s+story)?|story)$|"
    r"^[+\-]?\d+(?:[.,]\d+)?x$",
    re.I,
)
_JS_LIBRARY_ROWS = """
() => {
  const rows = Array.from(document.querySelectorAll("[role='row']"));
  const out = [];
  const idRx = /\\/(?:reel|videos)\\/(?:[^/"'?#]+\\/)?(\\d{10,})|[?&](?:v|id)=(\\d{10,})|"playable_id"\\s*:\\s*"(\\d{10,})"/g;
  for (const row of rows) {
    const cb = row.querySelector("[role='checkbox'], input[type='checkbox']");
    if (!cb) continue;
    const text = (row.innerText || "").replace(/\\s+/g, " ").trim();
    if (!text) continue;
    const hrefs = Array.from(row.querySelectorAll("a[href]")).map((a) => a.href || "");
    const cells = Array.from(row.querySelectorAll("[role='gridcell']"))
      .map((c) => (c.innerText || "").replace(/\\s+/g, " ").trim())
      .filter(Boolean);
    let thumb = "";
    let best = 0;
    for (const img of row.querySelectorAll("img")) {
      const src = img.currentSrc || img.src || img.getAttribute("src") || "";
      if (!src || src.startsWith("data:image/svg") || /emoji|rsrc\\.php/i.test(src)) continue;
      const area = (img.naturalWidth || img.width || 48) * (img.naturalHeight || img.height || 48);
      if (area >= best) {
        best = area;
        thumb = src;
      }
    }
    const htmlIds = [];
    const addId = (id) => {
      if (id && /^\\d{10,18}$/.test(id) && htmlIds.indexOf(id) < 0) htmlIds.push(id);
    };
    const html = row.innerHTML || "";
    let m;
    idRx.lastIndex = 0;
    while ((m = idRx.exec(html))) {
      addId(m[1] || m[2] || m[3]);
    }
    for (const img of row.querySelectorAll("img")) {
      const src = img.currentSrc || img.src || "";
      const mm = src.match(/(\\d{10,18})/g) || [];
      mm.forEach(addId);
    }
    const rk = Object.keys(row).find((k) => k.startsWith("__reactFiber") || k.startsWith("__reactInternalInstance"));
    if (rk) {
      const seen = new Set();
      const walk = (n, d) => {
        if (!n || d > 8 || htmlIds.length >= 8) return;
        if (typeof n === "object") {
          if (seen.has(n)) return;
          seen.add(n);
        }
        if (typeof n === "string") {
          if (/^\\d{10,18}$/.test(n)) addId(n);
          return;
        }
        if (typeof n !== "object") return;
        if (n.playable_id) addId(String(n.playable_id));
        if (n.video_id) addId(String(n.video_id));
        if (n.legacy_video_id) addId(String(n.legacy_video_id));
        const vals = Array.isArray(n) ? n : Object.values(n);
        for (const v of vals.slice(0, 30)) walk(v, d + 1);
      };
      try { walk(row[rk], 0); } catch (_e) {}
    }
    out.push({
      aria: row.getAttribute("aria-rowindex") || "",
      text,
      hrefs,
      cells,
      thumb,
      html_ids: htmlIds.slice(0, 8),
      checked: (cb.getAttribute("aria-checked") === "true") || !!cb.checked,
    });
  }
  return out;
}
"""
_JS_TICK_LIBRARY_ROWS = """
(wantedIds) => {
  const want = new Set((wantedIds || []).map((x) => String(x || "")).filter((x) => /^\\d{10,}$/.test(x)));
  if (!want.size) return { n: 0, ids: [] };
  const idRx = /\\/(?:reel|videos)\\/(?:[^/"'?#]+\\/)?(\\d{10,})|[?&](?:v|id)=(\\d{10,})|"playable_id"\\s*:\\s*"(\\d{10,})"/g;
  const hit = [];
  for (const row of document.querySelectorAll("[role='row']")) {
    const cb = row.querySelector("[role='checkbox'], input[type='checkbox']");
    if (!cb) continue;
    const blob = ((row.innerHTML || "") + " " + (row.innerText || "")).toString();
    let matched = "";
    idRx.lastIndex = 0;
    let m;
    while ((m = idRx.exec(blob))) {
      const id = m[1] || m[2] || m[3];
      if (want.has(id)) { matched = id; break; }
    }
    if (!matched) {
      const rk = Object.keys(row).find((k) => k.startsWith("__reactFiber") || k.startsWith("__reactInternalInstance"));
      if (rk) {
        const seen = new Set();
        const walk = (n, d) => {
          if (matched || !n || d > 8) return;
          if (typeof n === "object") {
            if (seen.has(n)) return;
            seen.add(n);
            const pid = n.playable_id || n.video_id || n.legacy_video_id;
            if (pid && want.has(String(pid))) { matched = String(pid); return; }
            const vals = Array.isArray(n) ? n : Object.values(n);
            for (const v of vals.slice(0, 28)) walk(v, d + 1);
            return;
          }
          if (typeof n === "string" && want.has(n)) matched = n;
        };
        try { walk(row[rk], 0); } catch (_e) {}
      }
    }
    if (!matched) {
      for (const id of want) {
        if (id && blob.indexOf(id) >= 0) { matched = id; break; }
      }
    }
    if (!matched) continue;
    const on = cb.getAttribute("aria-checked") === "true" || !!cb.checked;
    if (!on) {
      try { cb.click(); } catch (_e) {
        try { cb.dispatchEvent(new MouseEvent("click", { bubbles: true, cancelable: true, view: window })); } catch (_e2) {}
      }
    }
    if (hit.indexOf(matched) < 0) hit.push(matched);
  }
  return { n: hit.length, ids: hit };
}
"""
_JS_TICK_ROW_INDEXES = """
(indexes) => {
  const rows = Array.from(document.querySelectorAll("[role='row']"))
    .filter((row) => row.querySelector("[role='checkbox'], input[type='checkbox']"));
  let n = 0;
  for (const idx of indexes || []) {
    const row = rows[Number(idx)];
    if (!row) continue;
    const cb = row.querySelector("[role='checkbox'], input[type='checkbox']");
    if (!cb) continue;
    const on = cb.getAttribute("aria-checked") === "true" || !!cb.checked;
    if (!on) {
      try { cb.click(); } catch (_e) {
        try { cb.dispatchEvent(new MouseEvent("click", { bubbles: true, cancelable: true, view: window })); } catch (_e2) {}
      }
    }
    n += 1;
  }
  return n;
}
"""
_JS_CLICK_DELETE_POSTS = """
() => {
  const rx = /delete posts|delete post|xóa bài viết|xóa bài đăng|xóa các bài|move (?:posts )?to (?:bin|trash)|chuyển vào thùng rác/i;
  const skip = /comment|bình luận/i;
  const nodes = Array.from(document.querySelectorAll("[role='button'], button, [role='menuitem']"));
  for (const b of nodes) {
    const t = `${b.getAttribute("aria-label") || ""} ${b.innerText || ""}`.replace(/\\s+/g, " ").trim();
    if (!t || skip.test(t) || !rx.test(t)) continue;
    const r = b.getBoundingClientRect();
    if (r.width < 8 || r.height < 8) continue;
    try { b.click(); return t.slice(0, 80); } catch (_e) {}
  }
  return "";
}
"""
_JS_CHECKED_LIBRARY = """
() => document.querySelectorAll("[role='row'] [role='checkbox'][aria-checked='true'], [role='row'] input[type='checkbox']:checked").length
"""
_JS_CLICK_OWNER_POST_MENU = """
() => {
  const bad = (el) => {
    if (!el) return true;
    const root = el.closest("[aria-label], [role='article'], [role='complementary']") || el;
    const lab = ((el.getAttribute("aria-label") || "") + " " + (root.getAttribute("aria-label") || "")).toLowerCase();
    if (/comment|bình luận|reply|related/.test(lab)) return true;
    let p = el;
    for (let i = 0; i < 12 && p; i++) {
      const al = ((p.getAttribute && p.getAttribute("aria-label")) || "").toLowerCase();
      if (/comment|bình luận|reply/.test(al)) return true;
      p = p.parentElement;
    }
    return false;
  };
  const prefer = Array.from(document.querySelectorAll("[role='button'][aria-label], [aria-haspopup='menu'][role='button']"));
  for (const b of prefer) {
    const a = b.getAttribute("aria-label") || "";
    if (/actions for this post|manage (?:this )?post|tùy chọn bài|hành động cho bài|options for this post/i.test(a)) {
      if (typeof b.scrollIntoView === "function") b.scrollIntoView({ block: "center" });
      b.click();
      return "actions";
    }
  }
  const w = window.innerWidth || 1200;
  const ranked = prefer.filter((b) => {
    const a = (b.getAttribute("aria-label") || "").toLowerCase();
    if (!(a === "more" || a === "…" || a.includes("more") || a.includes("options") || a.includes("tùy chọn"))) return false;
    if (bad(b)) return false;
    const r = b.getBoundingClientRect();
    return r.width > 4 && r.height > 4 && r.top >= 0 && r.top < 360;
  }).sort((a, b) => {
    const ra = a.getBoundingClientRect();
    const rb = b.getBoundingClientRect();
    const score = (r) => (r.left > w * 0.42 ? 2 : 0) + (r.top < 220 ? 1 : 0);
    return score(rb) - score(ra);
  });
  if (ranked.length) {
    ranked[0].click();
    return "rail";
  }
  return "";
}
"""
_JS_SCROLL_LIBRARY = """
() => {
  const grid = document.querySelector("[role='grid']")
    || document.querySelector("[role='main']")
    || document.scrollingElement
    || document.documentElement;
  const ch = grid.clientHeight || window.innerHeight || 800;
  const step = Math.max(420, Math.floor(ch * 0.82));
  const before = grid.scrollTop || 0;
  try { grid.scrollBy(0, step); } catch (_e) { grid.scrollTop = before + step; }
  try { window.scrollBy(0, step); } catch (_e2) {}
  const sh = grid.scrollHeight || 0;
  const st = grid.scrollTop || 0;
  return {
    before, st, sh, ch,
    moved: Math.abs(st - before) > 6,
    atBottom: st + ch >= sh - 90,
  };
}
"""
_JS_EMBEDDED_IDS = """
() => {
  const ids = [];
  const seen = new Set();
  const add = (id) => {
    if (!id || id.length < 10 || seen.has(id)) return;
    seen.add(id);
    ids.push(id);
  };
  const reReel = /\\/reel\\/(\\d{10,})/g;
  const reVid = /\\/videos\\/(?:[^/\"']+\\/)?(\\d{10,})/g;
  const rePlay = /"playable_id"\\s*:\\s*"(\\d{10,})"/g;
  const scripts = document.querySelectorAll("script");
  for (const s of scripts) {
    const t = s.textContent || "";
    if (t.length < 40 || t.length > 450000) continue;
    let m;
    while ((m = reReel.exec(t))) add(m[1]);
    while ((m = reVid.exec(t))) add(m[1]);
    while ((m = rePlay.exec(t))) add(m[1]);
  }
  return ids.slice(0, 1500);
}
"""
_JS_HREF_VIDEOS = """
() => {
  const out = [];
  const seen = new Set();
  for (const a of document.querySelectorAll("a[href]")) {
    const h = a.href || a.getAttribute("href") || "";
    if (!h || !(h.includes("/reel/") || h.includes("/videos/") || h.includes("watch?") || /[?&]v=/.test(h))) continue;
    if (seen.has(h)) continue;
    seen.add(h);
    let img = a.querySelector("img");
    if (!img) {
      const box = a.closest("[role='listitem'], [role='article'], [role='gridcell']") || a.parentElement;
      img = box ? box.querySelector("img") : null;
    }
    const thumb = img ? (img.currentSrc || img.src || img.getAttribute("src") || "") : "";
    out.push({ href: h, thumb });
  }
  return out.slice(0, 900);
}
"""


def content_library_urls(fb_page_id: str = "") -> list[str]:
    """URL Content Library — gắn ``date_preset=lifetime`` để không kẹt 28 ngày."""
    aid = str(fb_page_id or "").strip()
    life = "date_preset=lifetime&start_date=2015-01-01"
    out: list[str] = [
        f"{_CONTENT_LIBRARY}?{life}",
        _CONTENT_LIBRARY,
    ]
    if aid.isdigit() and len(aid) >= 8:
        from src.automation.facebook_actions import default_meta_published_posts_url

        out.insert(0, f"{_CONTENT_LIBRARY}?asset_id={aid}&{life}")
        pub = default_meta_published_posts_url(aid)
        sep = "&" if "?" in pub else "?"
        out.append(f"{pub}{sep}{life}")
    return list(dict.fromkeys(out))


def is_noise_video_title(title: str) -> bool:
    """True nếu ô tiêu đề chỉ là nhiễu UI (tốc độ phát, ``--``, loại Reel/Video)."""
    t = str(title or "").strip()
    if not t or t in {"--", "—", "-", "Reel", "Reels", "Video", "Videos"}:
        return True
    return bool(_TITLE_NOISE.fullmatch(t))


def normalize_video_duration(raw: str) -> str:
    """Đưa thời lượng về ``m:ss`` để so khớp hàng thư viện với bản ghi quét."""
    s = str(raw or "").strip().casefold()
    if not s:
        return ""
    hour = minute = sec = 0
    mh = re.search(r"(\d+)\s*h", s)
    mm = re.search(r"(\d+)\s*m(?!s)", s)
    ms = re.search(r"(\d+)\s*s\b", s)
    if mh or mm or ms:
        hour = int(mh.group(1)) if mh else 0
        minute = int(mm.group(1)) if mm else 0
        sec = int(ms.group(1)) if ms else 0
        total = hour * 3600 + minute * 60 + sec
        return f"{total // 60}:{total % 60:02d}"
    mc = re.fullmatch(r"(\d{1,2}):(\d{2})(?::(\d{2}))?", s)
    if not mc:
        return ""
    if mc.group(3) is not None:
        total = int(mc.group(1)) * 3600 + int(mc.group(2)) * 60 + int(mc.group(3))
    else:
        total = int(mc.group(1)) * 60 + int(mc.group(2))
    return f"{total // 60}:{total % 60:02d}"


def published_search_needles(item: dict[str, str]) -> list[str]:
    """Các chuỗi ngày/giờ có thể xuất hiện trên hàng Content Library."""
    out: list[str] = []
    seen: set[str] = set()

    def _add(val: str) -> None:
        t = str(val or "").strip()
        if len(t) < 4 or t.casefold() in seen:
            return
        seen.add(t.casefold())
        out.append(t)

    _add(str(item.get("published_raw") or ""))
    _add(str(item.get("published") or ""))
    ts = str(item.get("published_ts") or "").strip()
    if ts.isdigit():
        try:
            dt = datetime.fromtimestamp(int(ts))
        except (OSError, OverflowError, ValueError):
            dt = None
        if dt is not None:
            _add(dt.strftime("%Y-%m-%d %H:%M"))
            _add(dt.strftime("%Y-%m-%d"))
            _add(f"{dt.strftime('%b')} {dt.day}")
            _add(f"{dt.strftime('%B')} {dt.day}")
            _add(dt.strftime("%H:%M"))
            clock12 = dt.strftime("%I:%M %p").lstrip("0")
            _add(clock12)
    return out


def parse_content_library_row(raw: dict[str, Any], *, page_url: str = "") -> dict[str, str] | None:
    """Chuẩn hoá một hàng Published trên Content Library thành bản ghi video."""
    del page_url
    text = str(raw.get("text") or "").strip()
    hrefs = [str(h) for h in (raw.get("hrefs") or []) if str(h).strip()]
    if not text:
        return None
    if _HEADER_ROW.search(text) and not _DURATION_RX.search(text) and "no text" not in text.casefold():
        return None
    vid = ""
    url = ""
    for href in hrefs:
        rec = classify_page_video_url(href)
        cand = rec.get("video_id") or ""
        if is_real_facebook_video_id(cand):
            vid = cand
            url = rec["url"]
            break
    if not vid:
        for hid in library_row_hit_ids(raw):
            vid = hid
            break
    if not is_real_facebook_video_id(vid):
        return None
    dur = ""
    m_dur = _DURATION_RX.search(text)
    if m_dur:
        dur = m_dur.group(0)
    cells = [str(c).strip() for c in (raw.get("cells") or []) if str(c).strip()]
    pub_raw = ""
    for cell in cells:
        ts_try, _lb = parse_facebook_publish_stamp(cell)
        if ts_try or _PUBLISHED_RX.search(cell):
            pub_raw = cell
            break
    if not pub_raw:
        m_pub = _PUBLISHED_RX.search(text)
        if m_pub:
            pub_raw = m_pub.group(0).strip()
    pub_ts, pub_lb = parse_facebook_publish_stamp(pub_raw)
    published = pub_lb or pub_raw
    title = ""
    for cell in cells:
        if cell == pub_raw or (dur and cell == dur):
            continue
        if is_noise_video_title(cell) or _DURATION_RX.fullmatch(cell):
            continue
        if "facebook.com/" in cell.casefold():
            continue
        title = cell
        break
    if not title:
        blob_l = text.casefold()
        if "no text" in blob_l:
            title = "No text content"
        elif "photo story" in blob_l:
            title = "Photo story"
        else:
            title = ""
    elif title.lower().startswith("no text"):
        title = "No text content"
    elif is_noise_video_title(title):
        title = ""
    if len(title) > 80:
        title = title[:77] + "…"
    kind = "Reel"
    blob = " ".join(hrefs).casefold() + " " + text.casefold()
    if "/videos/" in blob and "/reel/" not in blob:
        kind = "Video"
    if not url:
        url = (
            f"https://www.facebook.com/reel/{vid}"
            if kind == "Reel"
            else f"https://www.facebook.com/watch/?v={vid}"
        )
    thumb = str(raw.get("thumb") or raw.get("thumb_url") or "").strip()
    return {
        "video_id": vid,
        "kind": kind,
        "url": url,
        "title": title,
        "published": published,
        "published_ts": pub_ts,
        "published_raw": pub_raw,
        "duration": dur,
        "thumb_url": thumb,
        "row_sig": f"{title}|{published}|{dur}|{vid}",
    }


def library_row_hit_ids(raw: dict[str, Any]) -> set[str]:
    """Mọi ID video tìm được trên một hàng Content Library (href, HTML, ảnh)."""
    hits: set[str] = set()
    for href in raw.get("hrefs") or []:
        vid = video_id_from_url(str(href or ""))
        if is_real_facebook_video_id(vid):
            hits.add(vid)
    for hid in raw.get("html_ids") or []:
        cand = str(hid or "").strip()
        if is_real_facebook_video_id(cand):
            hits.add(cand)
    blob = " ".join(
        [
            str(raw.get("text") or ""),
            str(raw.get("thumb") or raw.get("thumb_url") or ""),
            " ".join(str(h) for h in (raw.get("hrefs") or [])),
        ]
    )
    for m in re.finditer(r"\d{10,18}", blob):
        cand = m.group(0)
        if is_real_facebook_video_id(cand):
            hits.add(cand)
    return hits


def library_row_matches_wanted(raw: dict[str, Any], wanted_ids: set[str]) -> bool:
    """True nếu hàng thư viện khớp một video_id đã chọn."""
    want = {str(x).strip() for x in (wanted_ids or set()) if is_real_facebook_video_id(x)}
    if not want:
        return False
    if library_row_hit_ids(raw) & want:
        return True
    blob = (
        str(raw.get("text") or "")
        + " "
        + str(raw.get("thumb") or "")
        + " "
        + " ".join(str(h) for h in (raw.get("hrefs") or []))
    )
    return any(vid in blob for vid in want)


def library_row_matches_item(raw: dict[str, Any], item: dict[str, str]) -> bool:
    """
    Khớp hàng Content Library với video đã quét:
    ID trong HTML/ảnh, hoặc thời lượng + mốc đăng, hoặc tiêu đề thật.
    """
    vid = str(item.get("video_id") or "").strip()
    if vid and library_row_matches_wanted(raw, {vid}):
        return True
    text = str(raw.get("text") or "")
    text_l = text.casefold()
    m_dur = _DURATION_RX.search(text)
    dur_row = normalize_video_duration(m_dur.group(0) if m_dur else "")
    dur_item = normalize_video_duration(str(item.get("duration") or ""))
    needles = [n.casefold() for n in published_search_needles(item) if len(n) >= 4]
    pub_hit = any(n in text_l for n in needles)
    if dur_item and dur_row and dur_item == dur_row and pub_hit:
        return True
    title = str(item.get("title") or "").strip()
    if (
        title
        and not is_noise_video_title(title)
        and title.casefold() not in {"no text content", "photo story"}
        and len(title) >= 8
        and title.casefold() in text_l
    ):
        return True
    return False


def parse_library_fingerprint(raw: dict[str, Any]) -> dict[str, str] | None:
    """Metadata hàng thư viện (kể cả khi chưa có video_id) — dùng để gắn vào bản ghi GraphQL."""
    text = str(raw.get("text") or "").strip()
    if not text:
        return None
    if _HEADER_ROW.search(text) and not _DURATION_RX.search(text) and "no text" not in text.casefold():
        return None
    m_dur = _DURATION_RX.search(text)
    dur = m_dur.group(0) if m_dur else ""
    cells = [str(c).strip() for c in (raw.get("cells") or []) if str(c).strip()]
    pub_raw = ""
    for cell in cells:
        ts_try, _lb = parse_facebook_publish_stamp(cell)
        if ts_try or _PUBLISHED_RX.search(cell):
            pub_raw = cell
            break
    if not pub_raw:
        m_pub = _PUBLISHED_RX.search(text)
        if m_pub:
            pub_raw = m_pub.group(0).strip()
    pub_ts, pub_lb = parse_facebook_publish_stamp(pub_raw)
    ids = library_row_hit_ids(raw)
    if not dur and not pub_raw and not ids:
        return None
    return {
        "video_id": next(iter(ids), ""),
        "duration": dur,
        "published_raw": pub_raw,
        "published_ts": pub_ts,
        "published": pub_lb or pub_raw,
        "title": "",
        "thumb_url": str(raw.get("thumb") or ""),
        "html_ids": ",".join(ids),
    }


def enrich_items_from_fingerprints(
    items: list[dict[str, str]],
    fingerprints: list[dict[str, str]],
) -> list[dict[str, str]]:
    """Gắn duration / published_raw từ hàng thư viện vào ID GraphQL."""
    rows = [dict(x) for x in items]
    by_id = {str(r.get("video_id") or ""): r for r in rows if is_real_facebook_video_id(r.get("video_id"))}
    for fp in fingerprints or []:
        target: dict[str, str] | None = None
        ids = {p for p in str(fp.get("html_ids") or "").split(",") if is_real_facebook_video_id(p)}
        vid = str(fp.get("video_id") or "")
        if is_real_facebook_video_id(vid):
            ids.add(vid)
        for cand in ids:
            if cand in by_id:
                target = by_id[cand]
                break
        if target is None:
            fp_dur = normalize_video_duration(str(fp.get("duration") or ""))
            fp_ts = str(fp.get("published_ts") or "")
            hits: list[dict[str, str]] = []
            if fp_dur and fp_ts.isdigit():
                for rec in rows:
                    same_dur = normalize_video_duration(str(rec.get("duration") or "")) == fp_dur
                    rts = str(rec.get("published_ts") or "")
                    same_time = rts.isdigit() and abs(int(rts) - int(fp_ts)) <= 180
                    if same_dur and same_time:
                        hits.append(rec)
                    elif not rec.get("duration") and same_time:
                        hits.append(rec)
            if len(hits) == 1:
                target = hits[0]
        if target is None:
            continue
        if fp.get("duration") and not target.get("duration"):
            target["duration"] = str(fp.get("duration") or "")
        if fp.get("published_raw") and not target.get("published_raw"):
            target["published_raw"] = str(fp.get("published_raw") or "")
        if fp.get("thumb_url") and not target.get("thumb_url"):
            target["thumb_url"] = str(fp.get("thumb_url") or "")
        if fp.get("published") and not target.get("published"):
            target["published"] = str(fp.get("published") or "")
    return rows


def candidate_video_open_urls(
    item: dict[str, str],
    *,
    fb_page_id: str = "",
    page_url: str = "",
) -> list[str]:
    """Các URL thử lần lượt khi xóa từng video (thư viện không tick được)."""
    vid = str(item.get("video_id") or "").strip()
    out: list[str] = []

    def _add(url: str) -> None:
        u = str(url or "").strip()
        if u and u not in out:
            out.append(u)

    _add(str(item.get("url") or ""))
    if not is_real_facebook_video_id(vid):
        return out
    _add(f"https://www.facebook.com/reel/{vid}")
    _add(f"https://www.facebook.com/watch/?v={vid}")
    fb = str(fb_page_id or "").strip()
    if fb.isdigit():
        _add(f"https://www.facebook.com/{fb}/videos/{vid}")
        _add(f"https://www.facebook.com/{fb}/reel/{vid}")
    m = re.search(r"facebook\.com/([^/?#]+)", str(page_url or ""), re.I)
    if m:
        slug = m.group(1)
        if slug and not slug.isdigit() and slug.casefold() not in {
            "watch",
            "reel",
            "reels",
            "videos",
            "profile.php",
            "professional_dashboard",
        }:
            _add(f"https://www.facebook.com/{slug}/videos/{vid}")
    return out


def is_page_post_delete_label(label: str) -> bool:
    """True nếu nhãn là xóa bài/video Page — không phải xóa bình luận."""
    s = str(label or "").strip()
    if not s:
        return False
    if re.search(r"comment|bình luận|reply", s, re.I):
        return False
    return bool(_DELETE_ITEM.search(s) or _DELETE_POSTS.search(s))


def sanitize_scanned_video_items(items: list[dict[str, str]]) -> list[dict[str, str]]:
    """Bỏ ID ghép từ UI, tiêu đề nhiễu; giữ thumbnail nếu có."""
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    for rec in items or []:
        row = dict(rec)
        vid = str(row.get("video_id") or video_id_from_url(str(row.get("url") or "")) or "").strip()
        if not is_real_facebook_video_id(vid) or vid in seen:
            continue
        seen.add(vid)
        row["video_id"] = vid
        title = str(row.get("title") or "").strip()
        if is_noise_video_title(title):
            row["title"] = ""
        if not row.get("url"):
            kind = str(row.get("kind") or "Reel")
            row["url"] = (
                f"https://www.facebook.com/reel/{vid}"
                if kind != "Video"
                else f"https://www.facebook.com/watch/?v={vid}"
            )
        out.append(row)
    return out


def thumbnail_cache_dir(fb_page_id: str = "") -> Path:
    """Thư mục cache ảnh thumbnail theo Page."""
    from src.utils.paths import project_root

    raw = re.sub(r"[^a-zA-Z0-9_-]+", "", str(fb_page_id or "").strip())[:64] or "page"
    dest = project_root() / "data" / "cache" / "page_video_thumbs" / raw
    dest.mkdir(parents=True, exist_ok=True)
    return dest


def _write_thumb_bytes(path: Path, data: bytes) -> bool:
    """Ghi JPEG nhỏ (khoảng 160px) để Treeview/preview nhẹ."""
    if not data or len(data) < 40:
        return False
    try:
        from PIL import Image

        im = Image.open(io.BytesIO(data))
        im = im.convert("RGB")
        im.thumbnail((160, 220))
        path.parent.mkdir(parents=True, exist_ok=True)
        im.save(path, "JPEG", quality=78)
        return path.is_file() and path.stat().st_size > 40
    except Exception:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data[:350_000])
            return path.is_file() and path.stat().st_size > 40
        except Exception:
            return False


def hydrate_video_thumbnails(
    page: Any,
    items: list[dict[str, str]],
    *,
    fb_page_id: str = "",
    status: StatusFn | None = None,
    cancel: threading.Event | None = None,
    limit: int = 180,
) -> list[dict[str, str]]:
    """Tải thumbnail về đĩa khi phiên Playwright còn mở (cookie Page)."""
    st = status or _noop
    stop = cancel or threading.Event()
    dest = thumbnail_cache_dir(fb_page_id)
    rows = [dict(x) for x in items or []]
    pending: list[dict[str, str]] = []
    for rec in rows:
        vid = str(rec.get("video_id") or "").strip()
        path = dest / f"{vid}.jpg"
        if path.is_file() and path.stat().st_size > 40:
            rec["thumb_path"] = str(path)
            continue
        url = str(rec.get("thumb_url") or "").strip()
        if vid and url.startswith("http"):
            pending.append(rec)
    pending = pending[: max(0, int(limit))]
    if not pending:
        return rows

    js_jobs = [{"id": r["video_id"], "url": r["thumb_url"]} for r in pending]
    fetched: dict[str, str] = {}
    for i in range(0, len(js_jobs), 12):
        if stop.is_set():
            break
        chunk = js_jobs[i : i + 12]
        try:
            part = page.evaluate(
                """
async (jobs) => {
  const out = {};
  await Promise.all((jobs || []).map(async (j) => {
    const id = String(j.id || '');
    const url = String(j.url || '');
    if (!id || !url) return;
    try {
      const r = await fetch(url, {credentials: 'include'});
      if (!r.ok) return;
      const blob = await r.blob();
      if (!blob || blob.size < 80 || blob.size > 350000) return;
      const buf = await blob.arrayBuffer();
      const bytes = new Uint8Array(buf);
      let bin = '';
      const step = 0x8000;
      for (let k = 0; k < bytes.length; k += step) {
        bin += String.fromCharCode.apply(null, bytes.subarray(k, k + step));
      }
      out[id] = btoa(bin);
    } catch (_e) {}
  }));
  return out;
}
""",
                chunk,
            )
            if isinstance(part, dict):
                fetched.update({str(k): str(v) for k, v in part.items() if v})
        except Exception as exc:  # noqa: BLE001
            logger.debug("[Xóa video] fetch thumbnail JS: {}", exc)
        st(f"Ảnh thumbnail: {min(i + 12, len(js_jobs))}/{len(js_jobs)}…")

    by_id = {str(r.get("video_id") or ""): r for r in rows}
    for rec in pending:
        if stop.is_set():
            break
        vid = str(rec.get("video_id") or "")
        path = dest / f"{vid}.jpg"
        raw_b64 = fetched.get(vid) or ""
        data = b""
        if raw_b64:
            try:
                data = base64.b64decode(raw_b64)
            except Exception:
                data = b""
        if not data:
            try:
                resp = page.context.request.get(str(rec.get("thumb_url") or ""), timeout=2_500)
                if getattr(resp, "ok", False):
                    data = resp.body() or b""
            except Exception:
                data = b""
        if _write_thumb_bytes(path, data):
            rec["thumb_path"] = str(path)
            if vid in by_id:
                by_id[vid]["thumb_path"] = str(path)
    saved = sum(1 for r in rows if r.get("thumb_path"))
    st(f"Đã lưu {saved}/{len(rows)} ảnh thumbnail.")
    return rows


def is_content_library_url(url: str) -> bool:
    """True nếu URL là Content Library / Published posts — không phải tab Reels/Videos."""
    cur = str(url or "").casefold()
    return any(
        k in cur
        for k in (
            "content_library",
            "published_posts",
            "professional_dashboard/content",
            "latest/posts",
        )
    )


def library_surface_ok(page: Any, *, fb_page_id: str = "", page_name: str = "") -> bool:
    """True nếu đang đứng Content Library — không lấy lưới Reels làm tín hiệu."""
    del page_name, fb_page_id
    return is_content_library_url(str(getattr(page, "url", "") or ""))


def _goto_library_fast(page: Any, url: str, *, timeout_ms: int = 22_000) -> None:
    """Mở URL thư viện: chỉ chờ ``domcontentloaded``, không chờ ``load`` (Business Suite hay treo)."""
    from src.automation.facebook_actions import assert_safe_facebook_navigation_url

    dest = str(url or "").strip()
    assert_safe_facebook_navigation_url(dest, label="content_library")
    try:
        page.goto(dest, wait_until="domcontentloaded", timeout=timeout_ms)
        return
    except Exception as exc:  # noqa: BLE001
        logger.warning("[Xóa video] goto Content Library timeout: {} — {}", dest, exc)
    try:
        page.evaluate("(u) => { window.location.assign(u); }", dest)
        page.wait_for_timeout(1_800)
    except Exception as exc:  # noqa: BLE001
        logger.warning("[Xóa video] location.assign Content Library: {}", exc)


def _library_grid_present(page: Any, *, timeout_ms: int = 8_000) -> bool:
    """Có hàng trên lưới Published / checkbox chọn bài."""
    deadline = time.monotonic() + max(0.6, timeout_ms / 1000.0)
    while time.monotonic() < deadline:
        try:
            if int(page.locator("[role='grid'] [role='row']").count()) >= 1:
                return True
            if int(page.locator("[role='row'] [role='checkbox']").count()) >= 1:
                return True
        except Exception:
            pass
        try:
            if page.get_by_text(
                re.compile(r"Content Library|Thư viện nội dung", re.I)
            ).first.is_visible(timeout=220):
                return True
        except Exception:
            pass
        try:
            page.wait_for_timeout(280)
        except Exception:
            time.sleep(0.28)
    return False


def _click_published_tab(page: Any) -> None:
    _click_role(page, "tab", _PUBLISHED_TAB, prefer_last=False) or _click_role(
        page, "link", _PUBLISHED_TAB, prefer_last=False
    )


def _apply_custom_history_range(page: Any, *, years: int = 10) -> bool:
    """Chọn Custom và điền từ 1/1/(năm hiện tại - years) → hôm nay."""
    if not (
        _click_role(page, "menuitem", _CUSTOM_RANGE, prefer_last=False)
        or _click_role(page, "option", _CUSTOM_RANGE, prefer_last=False)
        or _click_role(page, "button", _CUSTOM_RANGE, prefer_last=False)
    ):
        return False
    _human_pause(300, 600)
    end = datetime.now()
    start = end.replace(year=max(2009, end.year - max(5, years)), month=1, day=1)
    start_iso = start.strftime("%Y-%m-%d")
    end_iso = end.strftime("%Y-%m-%d")
    try:
        page.evaluate(
            """([a, b]) => {
              const labs = /start|begin|from|từ|end|to|đến|until|date/i;
              const inputs = Array.from(document.querySelectorAll("input"));
              const dated = inputs.filter((i) => {
                const blob = `${i.type||""} ${i.name||""} ${i.placeholder||""} ${i.getAttribute("aria-label")||""}`;
                return i.type === "date" || labs.test(blob);
              });
              if (!dated.length) return false;
              const setVal = (el, val) => {
                el.focus();
                el.value = val;
                el.dispatchEvent(new Event("input", { bubbles: true }));
                el.dispatchEvent(new Event("change", { bubbles: true }));
              };
              setVal(dated[0], a);
              if (dated[1]) setVal(dated[1], b);
              else if (dated[0]) setVal(dated[0], a);
              return true;
            }""",
            [start_iso, end_iso],
        )
    except Exception:
        pass
    if _click_role(page, "button", _APPLY_RANGE, prefer_last=True):
        return True
    try:
        page.keyboard.press("Enter")
        return True
    except Exception:
        return False


def _expand_library_date_range(page: Any, status: StatusFn) -> None:
    """Mở bộ lọc ngày → All time / Lifetime / Custom 10 năm (cả video 5 năm trước)."""
    opened = _click_role(page, "button", _DATE_RANGE_BTN, prefer_last=True)
    if not opened:
        try:
            loc = page.get_by_text(re.compile(r"Last\s+\d+\s+days|28 ngày", re.I)).first
            opened = click_locator_dom(loc, timeout_ms=1_500)
        except Exception:
            opened = False
    if not opened:
        status("Không thấy bộ lọc ngày Content Library — GraphQL vẫn nới lifetime.")
        return
    _human_pause(280, 550)
    if _click_role(page, "menuitem", _ALL_TIME_ITEM, prefer_last=False) or _click_role(
        page, "option", _ALL_TIME_ITEM, prefer_last=False
    ):
        status("Đã chọn All time / Lifetime trên Content Library.")
        try:
            page.wait_for_timeout(1_500)
        except Exception:
            time.sleep(1.5)
        return
    if _apply_custom_history_range(page, years=10):
        status("Đã đặt khoảng tùy chỉnh 10 năm — lấy cả video đăng 5 năm trước.")
        try:
            page.wait_for_timeout(1_600)
        except Exception:
            time.sleep(1.6)
        return
    try:
        page.keyboard.press("Escape")
    except Exception:
        pass
    status("Không chọn được All time — GraphQL route vẫn ép lifetime/10 năm.")


def _prefer_video_filter(page: Any, status: StatusFn) -> None:
    """Bật Filters → Video/Reel nếu có, để không dính bài ảnh."""
    if not _click_role(page, "button", re.compile(r"^Filters$|^Bộ lọc$", re.I), prefer_last=False):
        return
    _human_pause(220, 450)
    if _click_role(
        page,
        "menuitem",
        re.compile(r"Videos? and reels|Video.*[Rr]eel|Chỉ video", re.I),
        prefer_last=False,
    ) or _click_role(page, "checkbox", re.compile(r"^Videos?$|^Reels?$", re.I), prefer_last=False):
        status("Đã lọc Content Library theo Video/Reel.")
        page.wait_for_timeout(800)
        return
    try:
        page.keyboard.press("Escape")
    except Exception:
        pass


def switch_to_selected_page_fast(
    page: Any,
    *,
    page_url: str,
    page_name: str,
    status: StatusFn | None = None,
) -> None:
    """Vào đúng Page đã chọn — goto nhanh, không ``navigate_to_url`` (chờ load 90s)."""
    st = status or _noop
    from src.automation.facebook_actions import (
        _disable_view_only_guard,
        _failure_screenshot,
        _fb_normalize_client_url,
        _require_page_role_switched,
        _robust_switch_to_target_page,
        _view_only_guard_active_on_page,
    )

    dest = _fb_normalize_client_url(str(page_url or "").strip())
    if not dest:
        raise ValueError("Thiếu page_url.")
    st(f"Mở Page «{page_name or dest}»…")
    if _view_only_guard_active_on_page(page):
        _disable_view_only_guard(page)
    _goto_library_fast(page, dest, timeout_ms=25_000)
    try:
        page.wait_for_timeout(800)
    except Exception:
        time.sleep(0.8)
    if not _robust_switch_to_target_page(page, page_display_name=page_name, page_url=dest):
        _failure_screenshot(page, "page_video_switch_failed")
        raise RuntimeError(
            f"Không switch được sang vai trò Page «{page_name}». url={getattr(page, 'url', '')}"
        )
    _require_page_role_switched(
        page,
        page_display_name=page_name,
        page_url=dest,
        context="page_video_cleanup",
    )


def open_content_library_for_page(
    page: Any,
    *,
    fb_page_id: str,
    page_name: str,
    status: StatusFn | None = None,
    cancel: threading.Event | None = None,
) -> None:
    """Sau khi đã switch Page: mở Content Library (Published) — không dùng ``navigate_to_url`` (treo load)."""
    st = status or _noop
    stop = cancel or threading.Event()
    from src.automation.facebook_actions import _failure_screenshot

    last_err = ""
    for url in content_library_urls(fb_page_id):
        if stop.is_set():
            raise RuntimeError("Đã dừng theo yêu cầu.")
        short = url.split("?")[0]
        st(f"Mở Content Library ({short})…")
        _goto_library_fast(page, url, timeout_ms=22_000)
        try:
            page.wait_for_timeout(500)
        except Exception:
            time.sleep(0.5)
        cur = str(getattr(page, "url", "") or "").casefold()
        if "content_library" not in cur and "published_posts" not in cur:
            _click_role(page, "link", _LIBRARY_NAV, prefer_last=False)
            try:
                page.wait_for_timeout(400)
            except Exception:
                pass
        _click_published_tab(page)
        ready = _library_grid_present(page, timeout_ms=7_000) or library_surface_ok(
            page, fb_page_id=fb_page_id, page_name=page_name
        )
        if ready:
            _expand_library_date_range(page, st)
            _prefer_video_filter(page, st)
            st(f"Đã vào Content Library. {page.url}")
            return
        last_err = f"chưa thấy lưới Published ({getattr(page, 'url', '')})"
        logger.warning("[Xóa video] {} — thử URL kế.", last_err)
    _failure_screenshot(page, "page_video_content_library_missing")
    raise RuntimeError(
        "Không mở được Content Library của Page đã chọn. "
        + (last_err or f"url={getattr(page, 'url', '')}")
    )


def _raw_library_rows(page: Any) -> list[dict[str, Any]]:
    try:
        raw = page.evaluate(_JS_LIBRARY_ROWS)
    except Exception:
        return []
    if not isinstance(raw, list):
        return []
    return [x for x in raw if isinstance(x, dict)]


def _href_items_from_dom(
    page: Any,
    *,
    page_url: str,
    fb_page_id: str,
    allow_bare_reel: bool,
) -> list[dict[str, str]]:
    """Lấy link /reel/ /videos/ đang có trên DOM (bổ sung khi lưới ảo thiếu ID)."""
    try:
        hrefs = page.evaluate(_JS_HREF_VIDEOS)
    except Exception:
        hrefs = []
    live = str(getattr(page, "url", "") or "")
    out: list[dict[str, str]] = []
    for item in hrefs or []:
        if isinstance(item, dict):
            href = str(item.get("href") or "")
            thumb = str(item.get("thumb") or "").strip()
        else:
            href = str(item or "")
            thumb = ""
        h = _abs_facebook_href(href)
        if not href_belongs_to_page(
            h,
            expected_page_url=page_url,
            live_page_url=live,
            fb_page_id=fb_page_id,
            allow_bare_reel=allow_bare_reel,
        ):
            continue
        rec = classify_page_video_url(h)
        rec["title"] = rec.get("title") or ""
        rec["published"] = rec.get("published") or ""
        rec["source"] = "dom"
        if thumb:
            rec["thumb_url"] = thumb
        if is_real_facebook_video_id(rec.get("video_id")):
            out.append(rec)
    return out


def _embedded_script_items(page: Any, *, fb_page_id: str) -> list[dict[str, str]]:
    """Lấy playable_id /reel/ nhúng trong script (payload lần đầu, không cần cuộn)."""
    try:
        ids = page.evaluate(_JS_EMBEDDED_IDS)
    except Exception:
        ids = []
    page_id = str(fb_page_id or "").strip()
    out: list[dict[str, str]] = []
    for raw in ids or []:
        vid = str(raw or "").strip()
        if not is_real_facebook_video_id(vid) or vid == page_id:
            continue
        out.append(
            {
                "video_id": vid,
                "kind": "Reel",
                "url": f"https://www.facebook.com/reel/{vid}",
                "title": "",
                "published": "",
                "source": "embed",
            }
        )
    return out


def _click_library_see_more(page: Any) -> None:
    _click_role(
        page,
        "button",
        re.compile(r"see more|show more|load more|xem thêm|tải thêm", re.I),
        prefer_last=True,
    )


def collect_videos_from_content_library(
    page: Any,
    *,
    page_url: str,
    fb_page_id: str = "",
    page_name: str = "",
    max_videos: int = 400,
    status: StatusFn | None = None,
    cancel: threading.Event | None = None,
    harvest: NetworkVideoHarvest | None = None,
) -> list[dict[str, str]]:
    """Cuộn từng bước lưới Published + GraphQL — không nhảy cuối trang (bỏ lỡ phân trang)."""
    st = status or _noop
    stop = cancel or threading.Event()
    max_videos = max(10, int(max_videos or 400))
    rounds = max(80, min(220, max_videos // 6 + 50))
    if not library_surface_ok(page, fb_page_id=fb_page_id, page_name=page_name):
        open_content_library_for_page(
            page, fb_page_id=fb_page_id, page_name=page_name, status=st, cancel=stop
        )
    ordered: list[dict[str, str]] = []
    fingerprints: list[dict[str, str]] = []
    stable = 0
    prev = 0
    at_bottom_hits = 0
    for i in range(rounds):
        if stop.is_set() or len(ordered) >= max_videos:
            break
        rows = []
        for raw in _raw_library_rows(page):
            rec = parse_content_library_row(raw, page_url=page_url)
            if rec is not None:
                rec["source"] = rec.get("source") or "library"
                rows.append(rec)
            fp = parse_library_fingerprint(raw)
            if fp is not None:
                fingerprints.append(fp)
        href_rows = _href_items_from_dom(
            page, page_url=page_url, fb_page_id=fb_page_id, allow_bare_reel=True
        )
        net = harvest.items() if harvest is not None else []
        embed: list[dict[str, str]] = []
        if i == 0 or i % 8 == 7:
            embed = _embedded_script_items(page, fb_page_id=fb_page_id)
        ordered = merge_video_item_lists(rows, href_rows, net, embed)[:max_videos]
        st(
            f"Content Library: {len(ordered)}/{max_videos} video "
            f"(lưới {len(rows)}, GraphQL {len(net)}, cuộn {i + 1}/{rounds})."
        )
        snap = {"moved": True, "atBottom": False}
        try:
            snap = page.evaluate(_JS_SCROLL_LIBRARY) or snap
        except Exception:
            pass
        if i % 4 == 3:
            _click_library_see_more(page)
        try:
            page.keyboard.press("PageDown")
        except Exception:
            pass
        try:
            page.wait_for_timeout(480)
        except Exception:
            time.sleep(0.48)
        grew = len(ordered) > prev
        if grew:
            stable = 0
            at_bottom_hits = 0
        else:
            stable += 1
            if snap.get("atBottom"):
                at_bottom_hits += 1
            if stable >= 12 and at_bottom_hits >= 3:
                st(f"Hết trang Content Library — dừng ở {len(ordered)} video.")
                break
            if stable >= 18:
                st(f"Không thêm video sau {stable} vòng — dừng.")
                break
        prev = len(ordered)
    return enrich_items_from_fingerprints(ordered, fingerprints)[:max_videos]


def collect_videos_comprehensive(
    page: Any,
    *,
    page_url: str,
    fb_page_id: str = "",
    page_name: str = "",
    max_videos: int = 400,
    status: StatusFn | None = None,
    cancel: threading.Event | None = None,
    harvest: NetworkVideoHarvest | None = None,
) -> list[dict[str, str]]:
    """
    Quét toàn diện: Content Library (chính) + tab Reels/Videos của đúng Page + GraphQL.
    """
    st = status or _noop
    stop = cancel or threading.Event()
    max_videos = max(10, int(max_videos or 400))
    library = collect_videos_from_content_library(
        page,
        page_url=page_url,
        fb_page_id=fb_page_id,
        page_name=page_name,
        max_videos=max_videos,
        status=st,
        cancel=stop,
        harvest=harvest,
    )
    extra: list[dict[str, str]] = []
    if not stop.is_set():
        live = str(getattr(page, "url", "") or page_url)
        reels_url = normalize_facebook_reels_tab_url(page_url)
        videos_url = _profile_videos_tab_url(live if facebook_page_identity(live) else page_url)
        for label, dest, bare in (
            ("Reels", reels_url, True),
            ("Videos", videos_url, False),
        ):
            if stop.is_set() or len(merge_video_item_lists(library, extra)) >= max_videos:
                break
            if not dest or dest in ("https://www.facebook.com/",):
                continue
            st(f"Cuộn hết tab {label} (cả video cũ nhiều năm)…")
            _goto_library_fast(page, dest, timeout_ms=18_000)
            try:
                page.wait_for_timeout(800)
            except Exception:
                time.sleep(0.8)
            extra_rounds = max(80, min(360, max_videos // 5 + 40))
            stable = 0
            prev_n = 0
            for i in range(extra_rounds):
                if stop.is_set():
                    break
                extra.extend(
                    _href_items_from_dom(
                        page, page_url=page_url, fb_page_id=fb_page_id, allow_bare_reel=bare
                    )
                )
                n = len(merge_video_item_lists(library, extra, harvest.items() if harvest else []))
                st(f"Tab {label}: {n} video (cuộn {i + 1}/{extra_rounds}).")
                if n >= max_videos:
                    break
                if n == prev_n:
                    stable += 1
                    if stable >= 14:
                        break
                else:
                    stable = 0
                prev_n = n
                try:
                    page.evaluate(_JS_SCROLL_DEEP)
                except Exception:
                    try:
                        page.evaluate(_JS_SCROLL_SNAP)
                    except Exception:
                        pass
                try:
                    page.wait_for_timeout(420)
                except Exception:
                    time.sleep(0.42)
    merged = sanitize_scanned_video_items(
        sort_videos_by_published(
            merge_video_item_lists(library, extra, harvest.items() if harvest else []),
            newest_first=True,
        )
    )[:max_videos]
    if not library_surface_ok(page, fb_page_id=fb_page_id, page_name=page_name):
        try:
            open_content_library_for_page(
                page, fb_page_id=fb_page_id, page_name=page_name, status=st, cancel=stop
            )
        except Exception as exc:  # noqa: BLE001
            logger.debug("[Xóa video] quay lại Content Library: {}", exc)
    if not stop.is_set():
        st("Đang lấy ảnh thumbnail từng video…")
        merged = hydrate_video_thumbnails(
            page,
            merged,
            fb_page_id=fb_page_id,
            status=st,
            cancel=stop,
        )
    st(f"Quét xong: {len(merged)} video (thư viện + GraphQL + tab Page).")
    return merged


def _set_checkbox(loc: Any, *, want: bool) -> bool:
    try:
        state = str(loc.get_attribute("aria-checked") or "").lower()
        checked = state in ("true", "mixed") or bool(loc.is_checked())
    except Exception:
        checked = False
    if checked == want:
        return True
    return click_locator_dom(loc, timeout_ms=1_800)


def _library_select_all(page: Any) -> bool:
    loc = page.get_by_role("checkbox", name=_SELECT_ALL)
    if loc.count() > 0 and _set_checkbox(loc.first, want=True):
        return True
    header = page.locator("[role='columnheader'] [role='checkbox']")
    if header.count() > 0 and _set_checkbox(header.first, want=True):
        return True
    return False


def _library_tick_items(page: Any, items: list[dict[str, str]]) -> list[str]:
    """Tick hàng khớp ID, rồi fingerprint (thời lượng + ngày đăng)."""
    wanted = [it for it in items if is_real_facebook_video_id(it.get("video_id"))]
    if not wanted:
        return []
    hit = _library_tick_wanted(page, {str(it.get("video_id") or "") for it in wanted})
    found = {str(x) for x in hit}
    matched = list(hit)
    rows = _raw_library_rows(page)
    indexes: list[int] = []
    used: set[str] = set(found)
    for i, raw in enumerate(rows):
        for it in wanted:
            vid = str(it.get("video_id") or "")
            if vid in used or vid in found:
                continue
            if library_row_matches_item(raw, it):
                indexes.append(i)
                matched.append(vid)
                used.add(vid)
                break
    if not indexes:
        return matched
    try:
        n = int(page.evaluate(_JS_TICK_ROW_INDEXES, indexes) or 0)
    except Exception as exc:  # noqa: BLE001
        logger.debug("[Xóa video] tick theo chỉ số hàng: {}", exc)
        n = 0
    return matched if (n or hit) else []


def _library_set_search(page: Any, query: str) -> bool:
    """Gõ ô Search trên Content Library (lọc lưới trước khi tick)."""
    q = str(query or "").strip()
    if not q:
        return False
    loc = None
    try:
        box = page.get_by_role("searchbox")
        if int(box.count()) > 0:
            loc = box.first
    except Exception:
        loc = None
    if loc is None:
        try:
            box = page.locator("input[type='search'], input[placeholder*='Search' i], input[placeholder*='Tìm' i]")
            if int(box.count()) > 0:
                loc = box.first
        except Exception:
            loc = None
    if loc is None:
        return False
    try:
        loc.fill("")
        loc.fill(q)
        loc.press("Enter")
        page.wait_for_timeout(900)
        return True
    except Exception:
        try:
            click_locator_dom(loc, timeout_ms=1_200)
            page.evaluate(
                """(q) => {
                  const el = document.querySelector("input[type='search'], [role='searchbox']");
                  if (!el) return false;
                  el.focus();
                  el.value = q;
                  el.dispatchEvent(new Event('input', { bubbles: true }));
                  el.dispatchEvent(new Event('change', { bubbles: true }));
                  return true;
                }""",
                q,
            )
            page.wait_for_timeout(900)
            return True
        except Exception:
            return False


def _library_clear_search(page: Any) -> None:
    try:
        _library_set_search(page, "")
    except Exception:
        pass
    _press_escape(page)


def _library_tick_wanted(page: Any, wanted_ids: set[str]) -> list[str]:
    """Tick checkbox hàng khớp ID (HTML/ảnh/href). Trả list video_id đã tick trên viewport."""
    ids = [x for x in wanted_ids if is_real_facebook_video_id(x)]
    if not ids:
        return []
    try:
        raw = page.evaluate(_JS_TICK_LIBRARY_ROWS, ids)
    except Exception as exc:  # noqa: BLE001
        logger.debug("[Xóa video] JS tick hàng: {}", exc)
        raw = None
    hit: list[str] = []
    if isinstance(raw, dict):
        hit = [str(x) for x in (raw.get("ids") or []) if is_real_facebook_video_id(x)]
    if hit:
        return hit
    n = 0
    for row in _raw_library_rows(page):
        if not library_row_matches_wanted(row, set(ids)):
            continue
        n += 1
    if n <= 0:
        return []
    try:
        raw = page.evaluate(_JS_TICK_LIBRARY_ROWS, ids)
    except Exception:
        raw = None
    if isinstance(raw, dict):
        return [str(x) for x in (raw.get("ids") or []) if is_real_facebook_video_id(x)]
    return []


def _library_check_matching_rows(page: Any, wanted: set[str]) -> int:
    """Tick checkbox các hàng khớp video_id. Trả số hàng vừa tick."""
    return len(_library_tick_wanted(page, wanted))


def _press_escape(page: Any) -> None:
    try:
        page.evaluate("() => { document.dispatchEvent(new KeyboardEvent('keydown', {key:'Escape', bubbles:true, cancelable:true})); }")
    except Exception:
        try:
            page.keyboard.press("Escape")
        except Exception:
            pass


def _library_checked_count(page: Any) -> int:
    """Số checkbox hàng đang tick trên lưới Published."""
    try:
        return int(page.evaluate(_JS_CHECKED_LIBRARY) or 0)
    except Exception:
        return 0


def _confirm_delete_posts(page: Any, status: StatusFn) -> bool:
    """Bấm Delete posts trên thanh công cụ — không Escape (mất lựa chọn)."""
    checked = _library_checked_count(page)
    if checked <= 0:
        status("Chưa tick được hàng nào trên Content Library.")
        return False
    status(f"Đã tick {checked} hàng — bấm Delete posts…")
    clicked = False
    try:
        clicked = bool(page.evaluate(_JS_CLICK_DELETE_POSTS) or "")
    except Exception:
        clicked = False
    if not clicked:
        clicked = (
            _click_role(page, "button", _DELETE_POSTS, prefer_last=False)
            or _click_role(page, "menuitem", _DELETE_POSTS, prefer_last=False)
        )
    if not clicked:
        status("Không thấy nút Delete posts trên Content Library.")
        return False
    page.wait_for_timeout(500)
    confirmed = False
    try:
        dlg = page.get_by_role("dialog")
        if dlg.count() > 0:
            confirmed = _click_role(dlg.last, "button", _CONFIRM_BTN, prefer_last=False) or _click_role(
                dlg.last, "button", _DELETE_POSTS, prefer_last=False
            )
    except Exception:
        confirmed = False
    if not confirmed:
        try:
            confirmed = bool(
                page.evaluate(
                    """() => {
                      const rx = /^(delete|move|ok|xóa|chuyển|yes|move to bin)$/i;
                      const btns = Array.from(document.querySelectorAll("[role='dialog'] [role='button'], [role='dialog'] button"));
                      for (const b of btns) {
                        const t = (b.innerText || "").replace(/\\s+/g, " ").trim();
                        if (rx.test(t)) { b.click(); return true; }
                      }
                      return false;
                    }"""
                )
            )
        except Exception:
            confirmed = False
    if not confirmed:
        confirmed = _click_role(page, "button", _CONFIRM_BTN, prefer_last=False)
    page.wait_for_timeout(1_200)
    body = _page_hint_text(page)
    ok = confirmed or bool(_SUCCESS_HINT.search(body)) or _library_checked_count(page) < checked
    if ok:
        status("Đã xác nhận Delete posts trên Content Library.")
    return ok


def delete_from_content_library(
    page: Any,
    *,
    items: list[dict[str, str]],
    delete_all: bool,
    status: StatusFn | None = None,
    cancel: threading.Event | None = None,
) -> dict[str, Any]:
    """Tick hết hàng khớp rồi xóa một lần trên Content Library — không mở Watch."""
    st = status or _noop
    stop = cancel or threading.Event()
    _click_published_tab(page)
    wanted = {
        str(it.get("video_id") or "")
        for it in items
        if is_real_facebook_video_id(it.get("video_id"))
    }
    remaining = set(wanted)
    confirmed_ids: set[str] = set()
    ticked: set[str] = set()
    failed = 0
    skipped = 0
    if delete_all:
        if _library_select_all(page) and _confirm_delete_posts(page, st):
            confirmed_ids = set(wanted) or {"all"}
            remaining.clear()
        else:
            failed = 1
    else:
        stable = 0
        for rnd in range(36):
            if stop.is_set() or not remaining:
                skipped = len(remaining) if stop.is_set() else 0
                break
            batch = [it for it in items if str(it.get("video_id") or "") in remaining]
            hit = _library_tick_items(page, batch)
            if hit:
                ticked.update(hit)
                remaining -= set(hit)
                stable = 0
                st(f"Đã tick {len(ticked)}/{len(wanted)} video trên Content Library…")
            else:
                stable += 1
            if not remaining:
                break
            if stable >= 10:
                break
            try:
                page.evaluate(_JS_SCROLL_LIBRARY)
            except Exception:
                pass
            page.wait_for_timeout(280)
        if ticked or _library_checked_count(page) > 0:
            if _confirm_delete_posts(page, st):
                confirmed_ids = set(ticked)
                remaining = wanted - confirmed_ids
            else:
                failed = 1
                remaining = wanted
        elif remaining:
            st("Không khớp hàng Published với video đã chọn.")
    if remaining and not delete_all and not stop.is_set() and failed == 0:
        st("Lọc Search Content Library cho video còn lại…")
        for it in items:
            if stop.is_set():
                break
            vid = str(it.get("video_id") or "")
            if vid not in remaining:
                continue
            title = str(it.get("title") or "").strip()
            q = title if title and not is_noise_video_title(title) and title.casefold() not in {
                "no text content",
                "photo story",
            } else ""
            if not q or not _library_set_search(page, q):
                continue
            hit = _library_tick_items(page, [it])
            if not hit:
                continue
            st(f"Search «{q[:24]}» — xóa 1 video…")
            if _confirm_delete_posts(page, st):
                confirmed_ids.update(hit)
                remaining -= set(hit)
            else:
                failed += 1
                break
        _library_clear_search(page)
    if delete_all and confirmed_ids and failed == 0:
        deleted = len(items) or len(confirmed_ids)
        remaining.clear()
    else:
        deleted = len(confirmed_ids)
    results: list[dict[str, str]] = []
    for it in items:
        vid = str(it.get("video_id") or "")
        ok = vid in confirmed_ids or (delete_all and bool(confirmed_ids) and failed == 0)
        results.append(
            {
                "video_id": vid,
                "url": str(it.get("url") or ""),
                "kind": str(it.get("kind") or ""),
                "ok": "1" if ok else "0",
                "detail": "Content Library Delete posts" if ok else "Chưa xóa được trên Content Library",
            }
        )
    return {
        "deleted": deleted,
        "failed": failed,
        "skipped": skipped,
        "results": results,
        "remaining_ids": sorted(remaining),
    }


def _ingest_hrefs(
    page: Any,
    *,
    expected_url: str,
    fb_page_id: str,
    seen: set[str],
    ordered: list[dict[str, str]],
    max_videos: int,
) -> None:
    """Thu href trong ``[role=main]`` chỉ khi còn đúng Page đã chọn."""
    if len(ordered) >= max_videos:
        return
    live = str(getattr(page, "url", "") or "")
    on_page = browser_on_selected_page(page, expected_url, fb_page_id=fb_page_id)
    if not on_page:
        return
    hrefs = [_abs_facebook_href(h) for h in _collect_main_hrefs(page)]
    for href in hrefs:
        if not href_belongs_to_page(
            href,
            expected_page_url=expected_url,
            live_page_url=live,
            fb_page_id=fb_page_id,
            allow_bare_reel=True,
        ):
            continue
        rec = classify_page_video_url(href)
        key = rec["video_id"] or rec["url"].casefold()
        if not is_real_facebook_video_id(rec.get("video_id")):
            continue
        if not key or key in seen:
            continue
        seen.add(key)
        ordered.append(rec)
        if len(ordered) >= max_videos:
            return


def collect_videos_on_selected_page(
    page: Any,
    *,
    page_url: str,
    fb_page_id: str = "",
    page_name: str = "",
    max_videos: int = 400,
    status: StatusFn | None = None,
    cancel: threading.Event | None = None,
) -> list[dict[str, str]]:
    """Cuộn tab Reels rồi Videos của **Page đang mở** — không lấy ID JSON gợi ý."""
    st = status or _noop
    stop = cancel or threading.Event()
    max_videos = max(10, int(max_videos or 400))
    seen: set[str] = set()
    ordered: list[dict[str, str]] = []
    dest = str(page_url or "").strip()
    reels_url = normalize_facebook_reels_tab_url(dest)
    videos_url = _profile_videos_tab_url(dest)

    def _scroll_tab(label: str, rounds: int) -> None:
        stable = 0
        prev = 0
        for i in range(rounds):
            if stop.is_set() or len(ordered) >= max_videos:
                return
            if not browser_on_selected_page(page, dest, fb_page_id=fb_page_id, page_name=page_name):
                st(f"Lệch khỏi Page khi cuộn {label} — dừng tab này.")
                return
            _ingest_hrefs(
                page,
                expected_url=dest,
                fb_page_id=fb_page_id,
                seen=seen,
                ordered=ordered,
                max_videos=max_videos,
            )
            st(f"{label}: cuộn {i + 1}/{rounds} — {len(ordered)} video của Page.")
            if len(ordered) == prev:
                stable += 1
                if stable >= 8:
                    return
            else:
                stable = 0
            prev = len(ordered)
            try:
                page.evaluate(_JS_SCROLL_DEEP)
            except Exception:
                try:
                    page.evaluate(_JS_SCROLL_SNAP)
                except Exception:
                    pass
            time.sleep(0.55)

    from src.automation.facebook_actions import navigate_to_url

    st("Mở tab Reels của Page đã chọn…")
    navigate_to_url(page, reels_url)
    page.wait_for_timeout(1_200)
    if not browser_on_selected_page(page, dest, fb_page_id=fb_page_id, page_name=page_name):
        st("Tab Reels chưa đúng Page — điều hướng lại page_url.")
        navigate_to_url(page, dest)
        page.wait_for_timeout(1_400)
        navigate_to_url(page, normalize_facebook_reels_tab_url(str(page.url or dest)))
        page.wait_for_timeout(900)
    if not browser_on_selected_page(page, dest, fb_page_id=fb_page_id, page_name=page_name):
        raise RuntimeError(
            f"Không đứng đúng Page đã chọn khi quét Reels (đang ở {page.url}). "
            "Kiểm tra page_url / Meta Page ID và session tài khoản."
        )
    _scroll_tab("Reels", 48)

    if not stop.is_set() and len(ordered) < max_videos:
        live = str(page.url or dest)
        videos_url = _profile_videos_tab_url(live if facebook_page_identity(live) else dest)
        st("Mở tab Videos của cùng Page…")
        navigate_to_url(page, videos_url)
        page.wait_for_timeout(1_000)
        if browser_on_selected_page(page, dest, fb_page_id=fb_page_id, page_name=page_name):
            _scroll_tab("Videos", 36)
        else:
            st("Tab Videos lệch Page — bỏ qua (không lấy video trang khác).")
    return ordered


def delete_one_page_video(
    page: Any,
    item: dict[str, str],
    *,
    status: StatusFn | None = None,
    fb_page_id: str = "",
    page_url: str = "",
) -> dict[str, str]:
    """Xóa một video/reel — thử nhiều URL, chỉ bấm ⋯ của bài Page."""
    st = status or _noop
    url = str(item.get("url") or "").strip()
    vid = str(item.get("video_id") or video_id_from_url(url) or "").strip()
    label = vid or url
    urls = candidate_video_open_urls(item, fb_page_id=fb_page_id, page_url=page_url)
    if not urls:
        return {"ok": "0", "detail": "Thiếu URL video"}
    from src.automation.facebook_actions import _failure_screenshot, assert_safe_facebook_navigation_url, navigate_to_url

    last_exc = ""
    last_detail = "Không mở được video"
    for dest in urls:
        try:
            assert_safe_facebook_navigation_url(dest, label="xoa_video")
            st(f"Đang mở video {label}…")
            navigate_to_url(page, dest)
            page.wait_for_timeout(random.randint(800, 1_400))
        except Exception as exc:  # noqa: BLE001
            last_exc = str(exc)
            logger.warning("[Xóa video] Không mở được {}: {}", dest, exc)
            last_detail = f"Không mở được: {last_exc}"
            continue

        _press_escape(page)
        opened_more = False
        try:
            opened_more = bool(page.evaluate(_JS_CLICK_OWNER_POST_MENU))
        except Exception:
            opened_more = False
        if not opened_more:
            for sel in (
                "[aria-label='Actions for this post']",
                "[aria-label='Options for this post']",
                "[aria-label='More']",
            ):
                loc = page.locator(sel)
                try:
                    n = min(int(loc.count()), 4)
                except Exception:
                    n = 0
                for i in range(n):
                    btn = loc.nth(i)
                    try:
                        if btn.is_visible(timeout=250) and click_locator_dom(btn, timeout_ms=2_000):
                            opened_more = True
                            break
                    except Exception:
                        continue
                if opened_more:
                    break
        if not opened_more:
            last_detail = "Không thấy menu ⋯ của bài Page (không dùng ⋯ bình luận)"
            continue

        _human_pause(350, 800)
        clicked_del = _click_role(page, "menuitem", _DELETE_ITEM, prefer_last=False) or _click_role(
            page, "button", _DELETE_ITEM, prefer_last=False
        )
        if not clicked_del:
            last_detail = "Không thấy mục Xóa bài / Chuyển vào thùng rác"
            _press_escape(page)
            continue

        _human_pause(400, 900)
        confirmed = False
        try:
            dlg = page.get_by_role("dialog")
            if dlg.count() > 0:
                confirmed = _click_role(dlg.last, "button", _CONFIRM_BTN) or _click_role(page, "button", _CONFIRM_BTN)
            else:
                confirmed = _click_role(page, "button", _CONFIRM_BTN)
        except Exception:
            confirmed = _click_role(page, "button", _CONFIRM_BTN)

        _human_pause(600, 1_200)
        body = _page_hint_text(page)
        if _SUCCESS_HINT.search(body) or confirmed:
            logger.info("[Xóa video] Đã xóa {} ({})", label, item.get("kind") or "")
            return {"ok": "1", "detail": "Đã xóa / chuyển thùng rác"}
        last_detail = "Đã mở menu nhưng chưa xác nhận được hộp thoại xóa"
        _press_escape(page)

    _failure_screenshot(page, f"page_video_delete_failed_{vid or 'id'}")
    return {"ok": "0", "detail": last_detail}


class PageVideoCleanupRunner:
    """
    Một luồng Playwright: mở Page đã chọn, quét, rồi xóa trên cùng session.
    """

    def __init__(self) -> None:
        self._cmd: queue.Queue[tuple[str, dict[str, Any], queue.Queue[tuple[str, Any]]]] = queue.Queue()
        self._thread: threading.Thread | None = None
        self._factory: Any = None
        self._context: Any = None
        self._page: Any = None
        self._opened_key = ""
        self._cancel = threading.Event()
        self._harvest: NetworkVideoHarvest | None = None

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._loop, name="page-video-cleanup", daemon=True)
        self._thread.start()

    def cancel(self) -> None:
        self._cancel.set()

    def call(self, op: str, payload: dict[str, Any] | None = None, *, timeout: float = 900.0) -> Any:
        self.start()
        reply: queue.Queue[tuple[str, Any]] = queue.Queue()
        self._cmd.put((op, dict(payload or {}), reply))
        kind, val = reply.get(timeout=timeout)
        if kind == "err":
            raise val
        return val

    def shutdown_async(self) -> None:
        reply: queue.Queue[tuple[str, Any]] = queue.Queue()
        try:
            self._cmd.put(("shutdown", {}, reply))
        except Exception:
            pass

    def _loop(self) -> None:
        from src.automation.browser_factory import prepare_playwright_sync_thread

        prepare_playwright_sync_thread(label="page-video-cleanup")
        try:
            while True:
                op, payload, reply = self._cmd.get()
                if op == "shutdown":
                    self._close_browser()
                    reply.put(("ok", None))
                    return
                try:
                    if op == "scan":
                        self._cancel.clear()
                        reply.put(("ok", self._scan(payload)))
                    elif op == "delete":
                        self._cancel.clear()
                        reply.put(("ok", self._delete(payload)))
                    else:
                        reply.put(("err", RuntimeError(f"Lệnh không hỗ trợ: {op}")))
                except Exception as exc:  # noqa: BLE001
                    logger.exception("[Xóa video] {}", op)
                    reply.put(("err", exc))
        finally:
            self._close_browser()

    def _close_browser(self) -> None:
        from src.automation.browser_factory import sync_close_persistent_context

        if self._harvest is not None and self._page is not None:
            try:
                self._harvest.detach(self._page)
            except Exception:
                pass
        self._harvest = None
        sync_close_persistent_context(self._context, log_label="page-video-cleanup", same_thread=True)
        self._context = None
        self._page = None
        if self._factory is not None:
            try:
                self._factory.close()
            except Exception:
                pass
            self._factory = None
        self._opened_key = ""

    def _ensure_on_page(self, payload: dict[str, Any]) -> None:
        from src.automation.browser_factory import BrowserFactory
        from src.utils.account_proxy_mapper import prepare_account_dict_for_browser_run

        account = dict(payload.get("account") or {})
        target = resolve_page_cleanup_target(payload.get("page_row") or payload)
        page_url = target["page_url"]
        page_name = target["page_name"]
        fb_id = target["fb_page_id"]
        headless = True if payload.get("headless") is None else bool(payload.get("headless"))
        key = f"{account.get('id')}|{page_url}|{fb_id}|h{int(headless)}"
        st: StatusFn = payload.get("status") or _noop
        if self._page is not None and self._opened_key == key:
            if self._harvest is None:
                self._harvest = NetworkVideoHarvest(fb_page_id=fb_id)
                self._harvest.attach(self._page)
            if library_surface_ok(self._page, fb_page_id=fb_id, page_name=page_name):
                return
            st("Phiên còn mở — chuyển lại Page rồi vào Content Library…")
            switch_to_selected_page_fast(
                self._page, page_url=page_url, page_name=page_name, status=st
            )
            open_content_library_for_page(
                self._page,
                fb_page_id=fb_id,
                page_name=page_name,
                status=st,
                cancel=self._cancel,
            )
            return

        self._close_browser()
        mode = "ẩn" if headless else "có cửa sổ"
        st(f"1/4 Mở trình duyệt {mode}…")
        acc = prepare_account_dict_for_browser_run(account, require_proxy_live=True)
        self._factory = BrowserFactory(headless=headless, playwright_shared=True)
        self._context = self._factory.launch_persistent_context_from_account_dict(
            acc,
            headless=headless,
            disable_notifications=True,
            force_desktop_facebook=True,
            grid_viewport=(1600, 1000),
        )
        self._page = self._context.pages[0] if self._context.pages else self._context.new_page()
        try:
            self._page.set_default_timeout(8_000)
            self._page.set_default_navigation_timeout(25_000)
        except Exception:
            pass
        try:
            from playwright_stealth import Stealth

            Stealth().apply_stealth_sync(self._page)
        except Exception as exc:  # noqa: BLE001
            logger.debug("[Xóa video] stealth: {}", exc)
        st(f"2/4 Chuyển vai trò sang Page «{page_name or page_url}»…")
        switch_to_selected_page_fast(
            self._page, page_url=page_url, page_name=page_name, status=st
        )
        self._harvest = NetworkVideoHarvest(fb_page_id=fb_id)
        self._harvest.attach(self._page)
        st("3/4 Mở Content Library (Published)…")
        open_content_library_for_page(
            self._page,
            fb_page_id=fb_id,
            page_name=page_name,
            status=st,
            cancel=self._cancel,
        )
        self._opened_key = key
        st("4/4 Đã vào Content Library — bắt đầu đọc lưới video.")

    def _scan(self, payload: dict[str, Any]) -> dict[str, Any]:
        self._ensure_on_page(payload)
        target = resolve_page_cleanup_target(payload.get("page_row") or payload)
        st: StatusFn = payload.get("status") or _noop
        st("Quét toàn diện: Content Library + GraphQL + tab Reels/Videos của Page…")
        items = collect_videos_comprehensive(
            self._page,
            page_url=target["page_url"],
            fb_page_id=target["fb_page_id"],
            page_name=target["page_name"],
            max_videos=int(payload.get("max_videos") or 400),
            status=st,
            cancel=self._cancel,
            harvest=self._harvest,
        )
        if not items:
            return {
                "ok": False,
                "items": [],
                "message": (
                    f"Không thấy video của Page «{target['page_name'] or target['page_url']}». "
                    "Kiểm tra quyền Page, session, và thử bật «Hiện trình duyệt»."
                ),
            }
        net_n = len(self._harvest.items()) if self._harvest is not None else 0
        return {
            "ok": True,
            "items": items,
            "message": (
                f"Page «{target['page_name']}»: {len(items)} video/reel "
                f"(kèm {net_n} ID từ GraphQL)."
            ),
        }

    def _delete(self, payload: dict[str, Any]) -> dict[str, Any]:
        self._ensure_on_page(payload)
        target = resolve_page_cleanup_target(payload.get("page_row") or payload)
        st: StatusFn = payload.get("status") or _noop
        todo = [dict(x) for x in (payload.get("items") or []) if str(x.get("url") or x.get("video_id") or "").strip()]
        delete_all = bool(payload.get("delete_all"))
        if not todo and not delete_all:
            return {
                "ok": False,
                "deleted": 0,
                "failed": 0,
                "skipped": 0,
                "results": [],
                "message": "Chưa chọn video nào.",
            }
        st("Mở Content Library Published — xóa hàng loạt, không mở từng video.")
        try:
            open_content_library_for_page(
                self._page,
                fb_page_id=target["fb_page_id"],
                page_name=target["page_name"],
                status=st,
                cancel=self._cancel,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("[Xóa video] mở Content Library: {}", exc)
        lib = delete_from_content_library(
            self._page,
            items=todo,
            delete_all=delete_all,
            status=st,
            cancel=self._cancel,
        )
        deleted = int(lib.get("deleted") or 0)
        failed = int(lib.get("failed") or 0)
        skipped = int(lib.get("skipped") or 0)
        results = list(lib.get("results") or [])
        leftover_n = len([x for x in (lib.get("remaining_ids") or []) if x])
        if leftover_n and failed == 0:
            failed = leftover_n
        if leftover_n and deleted > 0:
            st(f"Đã xóa {deleted} trên Content Library. {leftover_n} video chưa khớp hàng Published.")
        msg = (
            f"Xóa xong trên Content Library «{target['page_name']}»: "
            f"{deleted} thành công, {failed} lỗi, {skipped} bỏ qua."
        )
        return {
            "ok": deleted > 0 and failed == 0,
            "deleted": deleted,
            "failed": failed,
            "skipped": skipped,
            "results": results,
            "message": msg,
        }


def scan_page_videos(
    *,
    page_url: str = "",
    page_row: dict[str, Any] | None = None,
    account: dict[str, Any],
    status: StatusFn | None = None,
    max_videos: int = 400,
    headless: bool | None = None,
    runner: PageVideoCleanupRunner | None = None,
) -> dict[str, Any]:
    """Quét video đúng Page đã chọn (một lần hoặc trên runner đang mở)."""
    own = runner is None
    run = runner or PageVideoCleanupRunner()
    row = dict(page_row or {})
    if page_url and not row.get("page_url"):
        row["page_url"] = page_url
    try:
        return run.call(
            "scan",
            {
                "page_row": row,
                "account": account,
                "status": status or _noop,
                "max_videos": max_videos,
                "headless": True if headless is None else bool(headless),
            },
        )
    finally:
        if own:
            run.shutdown_async()


def delete_page_videos(
    *,
    page_url: str = "",
    page_name: str = "",
    page_row: dict[str, Any] | None = None,
    account: dict[str, Any],
    items: list[dict[str, str]],
    status: StatusFn | None = None,
    cancel: threading.Event | None = None,
    headless: bool | None = True,
    runner: PageVideoCleanupRunner | None = None,
    delete_all: bool = False,
) -> dict[str, Any]:
    """Xóa video đã chọn trên đúng Page (tái sử dụng runner nếu có)."""
    own = runner is None
    run = runner or PageVideoCleanupRunner()
    row = dict(page_row or {})
    if page_url and not row.get("page_url"):
        row["page_url"] = page_url
    if page_name and not row.get("page_name"):
        row["page_name"] = page_name
    if cancel is not None and cancel.is_set():
        run.cancel()
    try:
        return run.call(
            "delete",
            {
                "page_row": row,
                "account": account,
                "items": items,
                "status": status or _noop,
                "headless": True if headless is None else bool(headless),
                "delete_all": delete_all,
            },
        )
    finally:
        if own:
            run.shutdown_async()
