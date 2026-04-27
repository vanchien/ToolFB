"""
Quét danh sách Page/Group từ tài khoản Facebook bằng Playwright (persistent context).

Trình tự:

1. ``https://www.facebook.com/pages/?category=your_pages&ref=bookmarks`` → trích xuất
   tất cả card «Pages you manage»: lấy *page_name* và *page_url*.
2. Với mỗi Page: cố gắng trích ``fb_page_id`` (asset_id) theo thứ tự:
   - Từ URL nếu chứa ID số (``/profile.php?id=...`` hoặc ``/<digits>``).
   - Tải HTML Page và regex trên embedded JSON: ``"pageID":"(\\d+)"``, ``"page_id":"(\\d+)"``,
     ``"entity_id":"(\\d+)"``.
3. Quét ``https://business.facebook.com/latest/home`` (và URL liên quan): tên, ID
   (từ ``page_id=`` / ``asset_id=`` trên link, hoặc từ ``data-surface`` dạng
   ``business_scope:page:<id>:<tên>`` ở modal lựa chọn tài sản), link profile khi
   có; gộp nhiều nguồn DOM.

API chính: :func:`scan_pages_for_account` (quét cũ) và
:mod:`src.automation.meta_business_scanner` (quét Business Suite theo portfolio).
"""

from __future__ import annotations

import re
import time
from collections.abc import Callable
from typing import TypedDict

from loguru import logger
from playwright.sync_api import BrowserContext, Page, TimeoutError as PlaywrightTimeoutError


StatusCallback = Callable[[str], None]


class ScannedPage(TypedDict, total=False):
    """Một bản ghi Page thu được từ scan."""

    page_name: str
    page_url: str
    fb_page_id: str
    page_kind: str
    source: str  # "your_pages" | "business_home" | "meta_business_suite" | ...
    business_name: str
    business_id: str
    account_id: str
    role: str


_YOUR_PAGES_URL = "https://www.facebook.com/pages/?category=your_pages&ref=bookmarks"
_BOOKMARKS_PAGES_URL = "https://www.facebook.com/bookmarks/pages"
_BUSINESS_HOME_URL = "https://business.facebook.com/latest/home"
_BUSINESS_PAGES_URL = "https://business.facebook.com/latest/posts/published_posts"

_FB_HOSTS = ("facebook.com", "www.facebook.com", "m.facebook.com", "web.facebook.com")
# Các slug KHÔNG phải page vanity (để loại ra khi quét link)
_BLACKLIST_SLUGS = {
    "", "home.php", "login", "logout.php", "messages", "notifications", "settings",
    "marketplace", "watch", "gaming", "groups", "events", "help", "policies",
    "privacy", "ads", "business", "pages", "bookmarks", "friends", "memories",
    "saved", "search", "stories", "reel", "reels", "profile.php",
}

_NUM_ID_RX = re.compile(r"(?:profile\.php\?id=|/)(\d{6,})(?:[/?#]|$)")
_PAGE_ID_JSON_RX = re.compile(
    r'"(?:pageID|page_id|entity_id|identifier)"\s*:\s*"(\d{6,})"'
)
_ASSET_ID_URL_RX = re.compile(r"[?&]asset_id=(\d{6,})")
# URL kiểu Business: ``?page_id=100895633077256&asset_id=100895633077256`` —
# ``page_id`` là chuẩn xác Meta Page ID, ưu tiên hơn ``asset_id`` (``asset_id`` có
# thể trỏ tới WhatsApp/IG asset).
_PAGE_ID_URL_RX = re.compile(r"[?&]page_id=(\d{6,})")


def _composer_url(pid: str) -> str:
    """URL Business composer để đăng bài trực tiếp (tool sẽ dùng làm ``page_url``).

    Có ``asset_id={Meta Page ID}`` — FB Business Suite sẽ mở composer cho đúng Page.
    """
    pid = str(pid or "").strip()
    if not pid:
        return ""
    return (
        "https://business.facebook.com/latest/composer/"
        f"?asset_id={pid}"
        "&nav_ref=internal_nav"
        "&ref=biz_web_content_manager_published_posts"
        "&context_ref=POSTS"
    )


def _noop_status(_msg: str) -> None:
    pass


def _extract_id_from_url(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return ""
    m = _NUM_ID_RX.search(u)
    return m.group(1) if m else ""


def _normalize_fb_url(href: str) -> str:
    h = (href or "").strip()
    if not h:
        return ""
    if h.startswith("//"):
        h = "https:" + h
    if h.startswith("/"):
        h = "https://www.facebook.com" + h
    # Bỏ fragment & tham số thừa (giữ nguyên path chính).
    return h.split("#", 1)[0]


def _is_candidate_page_url(href: str) -> bool:
    """URL có thể là trang Page/Group (không phải link hệ thống / bài viết)."""
    url = _normalize_fb_url(href)
    if not url.startswith("http"):
        return False
    try:
        from urllib.parse import urlparse
        p = urlparse(url)
    except Exception:
        return False
    if not any(p.netloc.endswith(h) for h in _FB_HOSTS):
        return False
    path = (p.path or "/").rstrip("/")
    if not path or path == "":
        return False
    segs = [s for s in path.split("/") if s]
    if not segs:
        return False
    first = segs[0].lower()
    if first in _BLACKLIST_SLUGS:
        return first == "profile.php" and "id=" in (p.query or "")
    # Bài post / reel thường có subpath "posts", "videos", "photos" ...
    if len(segs) >= 2 and segs[1].lower() in {"posts", "videos", "photos", "reels", "about"}:
        # Vẫn coi là Page (lấy segs[0]) nhưng sẽ tự chuẩn hoá về root.
        return True
    return True


def _canonical_page_url(href: str) -> str:
    """Chuẩn hoá về URL gốc của Page (bỏ /posts, /about …, bỏ query tracking)."""
    url = _normalize_fb_url(href)
    try:
        from urllib.parse import urlparse, urlunparse
        p = urlparse(url)
        segs = [s for s in (p.path or "/").split("/") if s]
        if segs and segs[0].lower() == "profile.php":
            return f"https://www.facebook.com/profile.php?id={_extract_id_from_url(url)}"
        if segs:
            base_path = "/" + segs[0] + "/"
        else:
            base_path = "/"
        return urlunparse((p.scheme or "https", p.netloc or "www.facebook.com", base_path, "", "", ""))
    except Exception:
        return url


_SCAN_CARDS_JS = r"""
() => {
  // Lượt 1 — Neo avatar: ``aria-label^="Profile picture for"`` (Business Home cổ điển).
  // Lượt 2 — Neo ``asset_id``: mỗi dòng thường có link
  //   ``/latest/...?asset_id=<Meta Page ID>`` (Messages). Leo tổ tiên nhỏ nhất vừa
  //   đủ chứa cả link ``www.facebook.com/<vanity>`` + ``asset_id`` → tên (ưu tiên
  //   ``svg[aria-label]``), href Page và số ID.
  // Lượt 3 — Neo ``data-surface`` (unified scoping / chọn tài sản, ``role="grid"``):
  //   chuỗi kiểu ``.../business_scope:page:<Meta Page ID>:<tên>`` — không cần link
  //   ``asset_id`` hay ``https://www.facebook.com/...`` trên cùng dòng.
  //
  // Trong cùng card: ưu tiên ``page_id=`` (Promote) nếu có, rồi ``asset_id=``.
  const ID_SEL = 'a[href*="page_id="], a[href*="asset_id="]';
  const PROF_SEL = 'a[aria-label^="Profile picture for"]';

  function pickNumericIdFromCard(cardRoot) {
    const idLinks = Array.from(cardRoot.querySelectorAll(ID_SEL));
    for (const l of idLinks) {
      const h = l.getAttribute('href') || '';
      const m = /[?&]page_id=(\d{6,})/.exec(h);
      if (m) return m[1];
    }
    for (const l of idLinks) {
      const h = l.getAttribute('href') || '';
      const m = /[?&]asset_id=(\d{6,})/.exec(h);
      if (m) return m[1];
    }
    return '';
  }

  function isNoisePageButtonText(t) {
    if (!t) return true;
    return /^(Messages|Promote|Create post|Share|Menu|Settings|Edit|more)$/i.test(t)
      || /notifications?/i.test(t);
  }

  function looksLikePageProfileHref(href) {
    if (!href) return false;
    if (/[?&](asset_id|page_id)=/i.test(href)) return false;
    if (/^https?:\/\/business\.facebook\.com/i.test(href)) return false;
    let u = href;
    if (u.startsWith('/') && !u.startsWith('//')) {
      u = 'https://www.facebook.com' + u;
    }
    if (!/^https?:\/\/(www\.|m\.|web\.)?facebook\.com\//i.test(u)) {
      return false;
    }
    const path = u.replace(/^https?:\/\/(www\.|m\.|web\.)?facebook\.com/i, '');
    if (!path || path === '/') return false;
    if (/^\/(latest|login|reg|help|stories|reel|watch|marketplace|groups|events|messages|settings)\b/i.test(path)) {
      return false;
    }
    if (/^\/profile\.php\?/i.test(path)) return true;
    const segs = path.split('/').filter(Boolean);
    return segs.length >= 1 && segs[0].length > 0;
  }

  function passFromProfilePics() {
    const out = [];
    const profAnchors = Array.from(document.querySelectorAll(PROF_SEL));
    for (const profA of profAnchors) {
      const aria = profA.getAttribute('aria-label') || '';
      const nameFromAria = aria.replace(/^Profile picture for\s*/i, '').trim();
      if (!nameFromAria) continue;
      let node = profA.parentElement;
      let cardRoot = null;
      for (let hop = 0; hop < 25 && node; hop++) {
        const profCount = node.querySelectorAll(PROF_SEL).length;
        if (profCount > 1) break;
        if (node.querySelector(ID_SEL)) { cardRoot = node; }
        node = node.parentElement;
      }
      if (!cardRoot) continue;
      const pid = pickNumericIdFromCard(cardRoot);
      if (!pid) continue;
      let displayName = nameFromAria;
      try {
        const cands = Array.from(cardRoot.querySelectorAll('a[href]'));
        for (const nl of cands) {
          const txt = (nl.innerText || nl.textContent || '').trim();
          if (txt && txt === nameFromAria) { displayName = txt; break; }
        }
        for (const s of cardRoot.querySelectorAll('svg[aria-label]')) {
          const al = (s.getAttribute('aria-label') || '').trim();
          if (al && (al === nameFromAria || al === displayName)) { displayName = al; break; }
        }
      } catch (_) {}
      out.push({ page_id: pid, name: displayName, href: profA.getAttribute('href') || '' });
    }
    return out;
  }

  function passFromAssetIdAnchors() {
    const out = [];
    const seenAid = new Set();
    const assetLinks = Array.from(document.querySelectorAll('a[href*="asset_id="]'));
    for (const mLink of assetLinks) {
      const hrefM = mLink.getAttribute('href') || '';
      const am = /[?&]asset_id=(\d{6,})/.exec(hrefM);
      if (!am) continue;
      const aid = am[1];
      if (seenAid.has(aid)) continue;
      let node = mLink;
      let cardRoot = null;
      for (let hop = 0; hop < 35; hop++) {
        node = node.parentElement;
        if (!node) break;
        if (!node.querySelector(`a[href*="asset_id=${aid}"]`)) continue;
        const hasPage = Array.from(node.querySelectorAll('a[href]')).some((a) => {
          return looksLikePageProfileHref(a.getAttribute('href') || '');
        });
        if (hasPage) { cardRoot = node; break; }
      }
      if (!cardRoot) continue;
      let displayName = '';
      for (const s of cardRoot.querySelectorAll('svg[aria-label]')) {
        const al = (s.getAttribute('aria-label') || '').trim();
        if (al && !/^facebook$/i.test(al) && al.length > 1) { displayName = al; break; }
      }
      let pageHref = '';
      if (!displayName) {
        for (const pa of cardRoot.querySelectorAll('a[href]')) {
          const h = pa.getAttribute('href') || '';
          if (!looksLikePageProfileHref(h)) continue;
          const t = (pa.innerText || pa.textContent || '').trim();
          if (t && !isNoisePageButtonText(t)) { displayName = t; pageHref = h; break; }
        }
      }
      if (!displayName) {
        for (const pa of cardRoot.querySelectorAll('a[href]')) {
          const h = pa.getAttribute('href') || '';
          if (!looksLikePageProfileHref(h)) continue;
          const t = (pa.innerText || pa.textContent || '').trim();
          if (t) { displayName = t; pageHref = h; break; }
        }
      } else {
        for (const pa of cardRoot.querySelectorAll('a[href]')) {
          const h = pa.getAttribute('href') || '';
          if (!looksLikePageProfileHref(h)) continue;
          const t = (pa.innerText || pa.textContent || '').trim();
          if (!pageHref) pageHref = h;
          if (t && t === displayName) { pageHref = h; break; }
        }
      }
      if (!displayName) continue;
      if (!pageHref) {
        for (const pa of cardRoot.querySelectorAll('a[href]')) {
          const h = pa.getAttribute('href') || '';
          if (looksLikePageProfileHref(h)) { pageHref = h; break; }
        }
      }
      let pid = pickNumericIdFromCard(cardRoot);
      if (!pid) pid = aid;
      seenAid.add(aid);
      out.push({ page_id: pid, name: displayName, href: pageHref || '' });
    }
    return out;
  }

  function passFromDataSurface() {
    const out = [];
    const seen = new Set();
    const nodes = Array.from(
      document.querySelectorAll('[data-surface*="business_scope:page:"]'),
    );
    function headingText(root) {
      if (!root) return '';
      const h = root.querySelector('[role="heading"]');
      return h ? (h.innerText || h.textContent || '').trim() : '';
    }
    for (const el of nodes) {
      const ds = (el.getAttribute('data-surface') || '').trim();
      // Ví: .../lib:business_scope:page:563468967184186:Xabre Owners Bandung
      const m = /business_scope:page:(\d{6,}):(.+)$/.exec(ds);
      if (!m) continue;
      const pid = m[1];
      if (seen.has(pid)) continue;
      let nameFromSurface = (m[2] || '').trim();
      const row = el.closest ? el.closest('[role="row"]') : null;
      // Ưu tiên heading trong hàng; không có thì trong chính gridcell (Meta thường lồng sâu).
      let displayName = headingText(row) || headingText(el);
      if (!displayName) displayName = nameFromSurface;
      if (!displayName) continue;
      let pageHref = '';
      const linkRoot = row || el;
      for (const a of linkRoot.querySelectorAll('a[href]')) {
        const h = a.getAttribute('href') || '';
        if (looksLikePageProfileHref(h)) {
          pageHref = h;
          break;
        }
      }
      seen.add(pid);
      out.push({ page_id: pid, name: displayName, href: pageHref });
    }
    return out;
  }

  function mergeRecord(r, merged) {
    if (!r || !r.page_id) return;
    const prev = merged.get(r.page_id);
    if (!prev) {
      merged.set(r.page_id, { ...r });
      return;
    }
    if ((r.name || '').length > (prev.name || '').length) { prev.name = r.name; }
    if (r.href && !prev.href) { prev.href = r.href; }
  }

  const merged = new Map();
  for (const r of passFromProfilePics()) {
    if (r && r.page_id) merged.set(r.page_id, { ...r });
  }
  for (const r of passFromAssetIdAnchors()) { mergeRecord(r, merged); }
  for (const r of passFromDataSurface()) { mergeRecord(r, merged); }
  return Array.from(merged.values());
}
"""

# Bản copy tham chiếu — :mod:`meta_business_scanner` trùng dùng cùng script trích DOM.
DOM_SCAN_CARDS_SCRIPT = _SCAN_CARDS_JS


def _collect_pages_from_your_pages(page: Page, status: StatusCallback) -> list[ScannedPage]:
    # Khoá chính: số Meta Page ID (ưu tiên ``page_id=`` trong card, rồi ``asset_id=``).
    seen_by_id: dict[str, ScannedPage] = {}
    seen_by_url: dict[str, ScannedPage] = {}

    def _scan_current(label: str) -> int:
        """Quét card trên DOM hiện tại, thêm vào ``seen_by_id``. Trả về số Page MỚI."""
        try:
            raw = page.evaluate(_SCAN_CARDS_JS)
        except Exception as exc:
            logger.warning("[PageScan] Eval {} lỗi: {}", label, exc)
            raw = []
        added = 0
        for item in (raw or []):
            pid = str(item.get("page_id") or "").strip()
            name = str(item.get("name") or "").strip()
            if not pid or not name:
                continue
            # page_url = Business Composer URL (dùng ``asset_id={Meta Page ID}``) — cho
            # phép tool mở thẳng trình soạn thảo khi đăng bài.
            composer = _composer_url(pid)
            existing = seen_by_id.get(pid)
            if existing is None:
                rec = ScannedPage(
                    page_name=name,
                    page_url=composer,
                    fb_page_id=pid,
                    page_kind="fanpage",
                    source=label,
                )
                seen_by_id[pid] = rec
                seen_by_url[composer.lower()] = rec
                added += 1
            else:
                if len(name) > len(existing.get("page_name", "")):
                    existing["page_name"] = name
                # Luôn ghi đè về composer URL nếu đã có pid (đồng nhất dữ liệu).
                existing["page_url"] = composer
        return added

    def _scroll_and_scan(
        label: str,
        *,
        max_rounds: int = 140,
        stable_rounds: int = 10,
        step_px: int = 2_200,
        pause_ms: int = 650,
    ) -> None:
        """Cuộn + scan INCREMENTAL: do FB virtualize danh sách nên card lướt qua sẽ bị
        gỡ khỏi DOM. Phải scan trong mỗi vòng để không bỏ sót.
        """
        stable = 0
        initial = _scan_current(label)
        status(f"{label}: quét ban đầu +{initial} Page (tổng {len(seen_by_id)}).")
        for i in range(max_rounds):
            # 1. Cuộn window.
            try:
                page.evaluate(f"window.scrollBy(0, {step_px})")
            except Exception:
                pass
            # 2. Cuộn các container có overflow auto/scroll.
            try:
                page.evaluate(
                    """(step) => {
                      const els = Array.from(document.querySelectorAll('*'));
                      for (const el of els) {
                        try {
                          const cs = getComputedStyle(el);
                          const oy = cs.overflowY;
                          if ((oy === 'auto' || oy === 'scroll')
                              && el.scrollHeight > el.clientHeight + 20) {
                            el.scrollTop = el.scrollTop + step;
                          }
                        } catch (_) {}
                      }
                    }""",
                    step_px,
                )
            except Exception:
                pass
            # 3. Thỉnh thoảng thử phím End để trigger load trang dài.
            if i % 10 == 9:
                try:
                    page.keyboard.press("End")
                except Exception:
                    pass
            try:
                page.wait_for_timeout(pause_ms)
            except Exception:
                pass
            # 4. Scan — đếm card mới.
            new_found = _scan_current(label)
            total = len(seen_by_id)
            status(f"{label}: cuộn {i + 1}/{max_rounds} (+{new_found} mới, tổng {total}).")
            if new_found == 0:
                stable += 1
                if stable >= stable_rounds:
                    break
            else:
                stable = 0
        # Cuộn ngược lên đầu + scan thêm 1 vài vòng để bắt card đã bị virtualize phía trên.
        try:
            page.evaluate("window.scrollTo(0, 0)")
            page.wait_for_timeout(900)
        except Exception:
            pass
        for _ in range(3):
            added_top = _scan_current(label)
            if added_top == 0:
                break
            try:
                page.evaluate(f"window.scrollBy(0, {step_px})")
                page.wait_for_timeout(pause_ms)
            except Exception:
                pass

    # Thứ tự quét: Business Home TRƯỚC (nguồn chứa page_id đầy đủ nhất — mỗi card
    # Business Home đều có nút Promote chứa ``page_id=<Meta Page ID>``), sau đó mới
    # đến Your Pages / Bookmarks (bổ sung Page có thể bị ẩn khỏi Business).
    for url, label in (
        (_BUSINESS_HOME_URL, "business_home"),
        (_BUSINESS_PAGES_URL, "business_published_posts"),
        (_YOUR_PAGES_URL, "your_pages"),
        (_BOOKMARKS_PAGES_URL, "bookmarks_pages"),
    ):
        status(f"Đang mở {label}…")
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=90_000)
        except Exception as exc:
            status(f"Không mở được {label}: {exc}")
            continue
        try:
            page.wait_for_load_state("networkidle", timeout=12_000)
        except PlaywrightTimeoutError:
            pass
        _scroll_and_scan(label)

    pages = list(seen_by_id.values())
    status(f"Tìm thấy {len(pages)} Page (gộp từ business_home + your_pages + bookmarks).")
    return pages


def _extract_fb_page_id_by_visit(page: Page, page_url: str) -> str:
    """Mở URL Page và lấy Meta Page ID theo thứ tự:

    1. Thử URL redirect cuối cùng có ``?page_id=`` (trang Business redirect).
    2. Embedded JSON: ``"pageID":"..."`` / ``"page_id":"..."`` / ``"entity_id":"..."``.
    """
    try:
        page.goto(page_url, wait_until="domcontentloaded", timeout=60_000)
    except Exception as exc:
        logger.debug("[PageScan] Visit {} lỗi: {}", page_url, exc)
        return ""
    try:
        page.wait_for_timeout(1_500)
    except Exception:
        pass
    try:
        cur_url = str(page.url or "")
    except Exception:
        cur_url = ""
    m_url = _PAGE_ID_URL_RX.search(cur_url) or _ASSET_ID_URL_RX.search(cur_url)
    if m_url:
        return m_url.group(1)
    try:
        html = page.content()
    except Exception:
        return ""
    m = _PAGE_ID_JSON_RX.search(html or "")
    return m.group(1) if m else ""


def scan_pages_for_account(
    context: BrowserContext,
    *,
    enrich_asset_id: bool = True,  # noqa: ARG001 — giữ tương thích API cũ (đã gộp vào _collect_pages_from_your_pages)
    visit_pages_for_missing_id: bool = True,
    max_visit_for_id: int = 60,
    status_cb: StatusCallback | None = None,
) -> list[ScannedPage]:
    """Quét toàn bộ Page/Group thuộc tài khoản đang đăng nhập trong ``context``.

    Args:
        context: BrowserContext persistent đã login sẵn.
        enrich_asset_id: Giữ lại để tương thích ngược với caller cũ. Hiện không dùng:
            việc quét Business Home đã được tích hợp sẵn trong
            :func:`_collect_pages_from_your_pages` (đi qua 4 URL gốc có chứa card Page).
        visit_pages_for_missing_id: Với Page còn thiếu ``fb_page_id``, mở từng URL để
            trích ID từ HTML (có thể chậm).
        max_visit_for_id: Giới hạn số Page mở từng trang để tránh quá lâu.
        status_cb: Callback nhận chuỗi trạng thái (hiển thị progress lên UI).

    Returns:
        Danh sách :class:`ScannedPage`. ``fb_page_id`` có thể rỗng nếu không trích được.
    """
    status = status_cb or _noop_status
    page = context.pages[0] if context.pages else context.new_page()

    pages = _collect_pages_from_your_pages(page, status)

    if visit_pages_for_missing_id:
        missing = [p for p in pages if not p.get("fb_page_id")]
        if missing:
            status(
                f"Còn {len(missing)} Page thiếu fb_page_id — mở từng trang để trích (tối đa {max_visit_for_id})."
            )
            for i, p in enumerate(missing[:max_visit_for_id]):
                status(f"[{i + 1}/{len(missing[:max_visit_for_id])}] {p.get('page_name', '')}")
                pid = _extract_fb_page_id_by_visit(page, p.get("page_url", ""))
                if pid:
                    p["fb_page_id"] = pid
                # Nhịp nhẹ tránh dính rate-limit.
                time.sleep(0.6)

    status(
        "Hoàn tất: {} Page (có fb_page_id: {}).".format(
            len(pages), sum(1 for p in pages if p.get("fb_page_id"))
        )
    )
    return pages
