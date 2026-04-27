"""
Tiện ích chung cho **Meta Business Suite** (panel chọn tài sản / portfolio).

* Bộ chọn scope, danh sách portfolio cột trái, click theo tên + \"N business assets\".
* Thu thứ tự Page từ ``data-surface`` (``business_scope:page:…``) qua ``_ordered_page_rows_from_scope``.

Luồng quét người dùng cuối nằm ở :mod:`mbs_page_scanner` (``run_mbs_full_scan``).
Không dùng Graph API.
"""

from __future__ import annotations

import random
import re
import time
import unicodedata
from collections.abc import Callable
from typing import Any
from loguru import logger
from playwright.sync_api import BrowserContext, Page

from src.automation.facebook_page_scanner import ScannedPage

StatusCallback = Callable[[str], None]

_BUSINESS_ENTRY = re.compile(r"(\d+)\s+business assets", re.I)
_PLACEHOLDER_PATTERNS = (
    re.compile(r"Search for a business asset", re.I),
    re.compile(r"Search for", re.I),
)

_LIST_PORTFOLIOS_JS = r"""
() => {
  // Cột trái: [role=grid] có «Business portfolios»/«Your account» → từng [role=row] + «N business assets».
  // Loại lưới cột phải (chỉ Facebook Page) bằng cách bắt buộc nội dung 2 section tài sản cột trái.
  const dialog = document.querySelector('[role="dialog"]') || document.body;
  const re = /(\d+)\s+business assets/i;
  const seen = new Set();
  const out = [];

  function businessIdFromEl(el) {
    if (!el) { return ""; }
    const ds0 = el.getAttribute && el.getAttribute("data-surface");
    const ch = el.querySelector && el.querySelector("[data-surface]");
    const ds = ds0 || (ch && ch.getAttribute("data-surface")) || "";
    const ma = /:(?:page|portfolio|bm):(\d{6,}):/i.exec(ds);
    return ma ? ma[1] : "";
  }

  function nameFromRow(row) {
    const heads = row.querySelectorAll('[role="heading"]');
    for (let j = heads.length - 1; j >= 0; j--) {
      const hn = (heads[j].innerText || "").replace(/\r/g, "").replace(/\s+/g, " ").trim();
      if (!hn) { continue; }
      if (/^Business portfolios$/i.test(hn) || /^Your account$/i.test(hn)) { continue; }
      if (hn.length > 200) { continue; }
      return hn;
    }
    const lines = (row.innerText || "").split(/\n/).map((s) => s.trim()).filter(Boolean);
    for (let i = 0; i < lines.length; i++) {
      if (lines[i].match(re) && i > 0) {
        let cand = lines[i - 1];
        if (cand.length < 2 && i > 1) { cand = lines[i - 2]; }
        if (!/^Business portfolios$/i.test(cand) && !/^Your account$/i.test(cand)) { return cand; }
      }
    }
    return "";
  }

  for (const grid of dialog.querySelectorAll('[role="grid"]')) {
    const gText = (grid.innerText || "").slice(0, 4000);
    if (!/Business portfolios|Your account/i.test(gText)) { continue; }
    for (const row of grid.querySelectorAll('[role="row"]')) {
      const t = (row.innerText || "").replace(/\r/g, "");
      if (!re.test(t)) { continue; }
      const m = t.match(re);
      if (!m) { continue; }
      const n = parseInt(m[1], 10);
      if (Number.isNaN(n) || n < 0) { continue; }
      const name = nameFromRow(row);
      if (!name) { continue; }
      if (/^Create a business portfolio$/i.test(name)) { continue; }
      if (/^Business portfolios$/i.test(name) || /^Your account$/i.test(name)) { continue; }
      const key = name + "\0" + m[1];
      if (seen.has(key)) { continue; }
      seen.add(key);
      const gc = row.querySelector('[role="gridcell"]') || row;
      out.push({ name, count: m[1], business_id: businessIdFromEl(gc) || businessIdFromEl(row) || "" });
    }
  }
  if (out.length) { return out; }

  function parseRow(el) {
    const txt = (el.innerText || el.textContent || "");
    if (!re.test(txt)) { return null; }
    let name = "";
    let count = "";
    for (const h of el.querySelectorAll('[role="heading"]')) {
      const hn = (h.innerText || "").replace(/\r/g, "").replace(/\s+/g, " ").trim();
      if (!hn || /^Business portfolios$/i.test(hn) || /^Your account$/i.test(hn)) { continue; }
      const m0 = txt.match(re);
      if (m0) { name = hn; count = m0[1]; break; }
    }
    if (!name) {
      const lines2 = txt.split(/\n/).map((s) => s.trim()).filter(Boolean);
      for (let i = 0; i < lines2.length; i++) {
        const mm = lines2[i].match(re);
        if (mm) {
          count = mm[1];
          if (i > 0) {
            name = lines2[i - 1];
            if (name.length === 1 && i > 1) { name = lines2[i - 2]; }
          }
          break;
        }
      }
    }
    if (!name) { return null; }
    if (name.length > 200) { return null; }
    if (/^Create a business portfolio$/i.test(name)) { return null; }
    if (/^Business portfolios$/i.test(name) || /^Your account$/i.test(name)) { return null; }
    if (!count) { return null; }
    const n2 = parseInt(count, 10);
    if (Number.isNaN(n2) || n2 < 0) { return null; }
    return { el, name, count };
  }

  const candidates = new Set();
  function addIf(text, el) {
    if (re.test(String(text || ""))) { candidates.add(el); }
  }
  for (const cell of dialog.querySelectorAll(
    '[role="grid"] [role="gridcell"], [role="listitem"], [role="option"]',
  )) {
    addIf(cell.innerText, cell);
  }
  if (candidates.size === 0) {
    for (const btn of dialog.querySelectorAll('[role="button"], [tabindex="0"]')) {
      addIf(btn.innerText, btn);
    }
  }
  for (const btn of candidates) {
    const row2 = parseRow(btn);
    if (!row2) { continue; }
    const { name, count, el: btn0 } = row2;
    const key = name + "\0" + count;
    if (seen.has(key)) { continue; }
    seen.add(key);
    out.push({ name, count, business_id: businessIdFromEl(btn0) || "" });
  }
  return out;
}
"""

_CLICK_BUSINESS_BY_NAME_COUNT_JS = r"""
(name, count) => {
  const dialog = document.querySelector('[role="dialog"]') || document.body;
  const re = new RegExp(String(count) + "\\s+business assets", "i");
  const norm = (s) => String(s || "").replace(/\r/g, "").replace(/\s+/g, " ").trim();
  const nameN = norm(name);
  if (!nameN) return false;
  const countW = String(count);
  if (countW === "0" || parseInt(countW, 10) < 1) return false;

  const fold = (s) => {
    try {
      return norm(s).normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLowerCase();
    } catch (_) {
      return norm(s).toLowerCase();
    }
  };
  const alnum = (s) => norm(s).replace(/[^a-zA-Z0-9\u00C0-\u024F]/g, "").toLowerCase();
  const namesLoose = (a, b) => {
    const A = norm(a);
    const B = norm(b);
    if (A === B) return true;
    if (A.toLowerCase() === B.toLowerCase()) return true;
    if (fold(a) === fold(b)) return true;
    const ca = alnum(a);
    const cb = alnum(b);
    if (ca && cb && ca === cb) return true;
    const minLen = 5;
    if (A.length > minLen && B.length > minLen) {
      const la = A.toLowerCase();
      const lb = B.toLowerCase();
      if (la.includes(lb) || lb.includes(la)) return true;
    }
    return false;
  };
  const nameMatchesLine = (line) => namesLoose(line, nameN);
  const nameMatchesInRow = (btn) => {
    for (const h of btn.querySelectorAll('[role="heading"]')) {
      if (nameMatchesLine(h.innerText || "")) return true;
    }
    for (const line of String(btn.innerText || "")
      .split("\n")
      .map((s) => s.trim())
      .filter(Boolean)) {
      if (nameMatchesLine(line)) return true;
    }
    return false;
  };

  const tryClick = (el) => {
    let n = el;
    const innerGc = el && el.querySelector && el.querySelector('[role="gridcell"]');
    if (innerGc) { n = innerGc; }
    else if (el && el.closest) {
      const r = el.closest('[role="row"]');
      if (r) {
        const gc2 = r.querySelector('[role="gridcell"]');
        if (gc2) { n = gc2; }
        else { n = r; }
      }
    }
    try {
      n.scrollIntoView({ block: "center", inline: "nearest" });
    } catch (_) {}
    try {
      if (n.click) { n.click(); }
    } catch (_) {}
    try {
      const r = n.getBoundingClientRect();
      if (r.width > 2 && r.height > 2) {
        const cx = r.left + r.width / 2;
        const cy = r.top + r.height / 2;
        const mk = (type) =>
          new MouseEvent(type, {
            bubbles: true,
            cancelable: true,
            view: window,
            clientX: cx,
            clientY: cy,
          });
        n.dispatchEvent(mk("mousedown"));
        n.dispatchEvent(mk("mouseup"));
        n.dispatchEvent(mk("click"));
      }
    } catch (_) {}
    return true;
  };

  for (const grid of dialog.querySelectorAll('[role="grid"]')) {
    if (!/Business portfolios|Your account/i.test((grid.innerText || "").slice(0, 4000))) {
      continue;
    }
    for (const row of grid.querySelectorAll('[role="row"]')) {
      const t = String(row.innerText || "");
      if (!re.test(t)) { continue; }
      if (nameMatchesInRow(row)) {
        return tryClick(row);
      }
    }
  }

  const nodeSel = [
    '[role="gridcell"]',
    '[role="row"]',
    '[role="button"]',
    '[tabindex="0"]',
    '[role="option"]',
    '[role="listitem"]',
    "div[data-surface*='business_scope']",
  ].join(",");
  const nodes = Array.from(dialog.querySelectorAll(nodeSel));
  for (const btn of nodes) {
    const txt = String(btn.innerText || btn.textContent || "");
    if (!re.test(txt)) continue;
    for (const h of btn.querySelectorAll('[role="heading"]')) {
      if (nameMatchesLine(h.innerText || "")) {
        return tryClick(btn);
      }
    }
    const lines = txt.split(/\n/).map((s) => s.trim()).filter(Boolean);
    for (let i = 0; i < lines.length; i++) {
      const m = lines[i].match(/(\d+)\s+business assets/i);
      if (!m || m[1] !== countW) continue;
      for (let j = 0; j < i; j++) {
        if (nameMatchesLine(lines[j])) {
          return tryClick(btn);
        }
      }
    }
    if (nameMatchesInRow(btn)) {
      return tryClick(btn);
    }
  }
  for (const row of dialog.querySelectorAll('[role="row"]')) {
    const t = String(row.innerText || "");
    if (!re.test(t)) continue;
    if (nameMatchesInRow(row) || t.split(/\n/).some((ln) => nameMatchesLine(ln.trim()))) {
      return tryClick(row);
    }
  }
  return false;
}
"""


def _noop_status(_msg: str) -> None:
    pass


def _human_delay() -> None:
    time.sleep(random.uniform(0.35, 0.8))


_ORDERED_PAGE_SCAN_JS = r"""
() => {
  const dialog = document.querySelector('[role="dialog"]') || document.body;
  const out = [];
  const seen = new Set();
  for (const n of dialog.querySelectorAll("[data-surface]")) {
    const ds = n.getAttribute("data-surface") || "";
    const m = /business_scope:page:(\d{6,}):/.exec(ds);
    if (!m) { continue; }
    const id = m[1];
    if (seen.has(id)) { continue; }
    seen.add(id);
    let name = "";
    const row = n.closest('[role="row"]');
    if (row) {
      const h = row.querySelector('[role="heading"]');
      if (h) { name = (h.innerText || "").trim(); }
      if (!name) {
        for (const line of (row.innerText || "").split("\n").map(s => s.trim()).filter(Boolean)) {
          if (/^Facebook Page$/i.test(line)) { continue; }
          if (line && !/^\d+\s+business assets$/i.test(line) && !/^Facebook$/i.test(line)) {
            name = line;
            break;
          }
        }
      }
    }
    if (!name) {
      const tail = (ds.split("page:" + id + ":")[1] || "");
      const part = tail.split("/")[0].split(":")[0].trim();
      if (part) { name = part; }
    }
    out.push({ id, name: name || "" });
  }
  return out;
}
"""


def _ordered_page_rows_from_scope(p: Page) -> list[dict[str, str]]:
    try:
        raw = p.evaluate(_ORDERED_PAGE_SCAN_JS)
    except Exception as exc:  # noqa: BLE001
        logger.debug("[MetaScan] ordered page rows: {}", exc)
        return []
    if not isinstance(raw, list):
        return []
    return [x for x in raw if isinstance(x, dict) and (x or {}).get("id")]


_SCOPE_OPEN_VISIBLE_JS = r"""
() => {
  if (document.querySelector('[data-surface*="business_unified_scoping_selector"]')) {
    return true;
  }
  const root = document.querySelector('[role="dialog"]') || document.body;
  if (!root) return false;
  for (const el of root.querySelectorAll("input, textarea, [role='searchbox']")) {
    const ph = (el.getAttribute("placeholder") || "");
    const al = (el.getAttribute("aria-label") || "");
    const t = (ph + " " + al).toLowerCase();
    if (t.includes("search") && (t.includes("business") || t.includes("asset"))) {
      return true;
    }
  }
  const body = root;
  const txt = (body.innerText || "");
  if (/\d+\s+business assets/i.test(txt) && /(Business portfolios|Your account|Facebook Page)/i.test(txt)) {
    return true;
  }
  return false;
}
"""

_CLICK_ASSET_SCOPE_TRIGGER_JS = r"""
() => {
  const tryClick = (el) => {
    try {
      el.dispatchEvent(new MouseEvent("click", { bubbles: true, cancelable: true, view: window }));
    } catch (_) {}
    try {
      el.click();
    } catch (_) {
      return false;
    }
    return true;
  };
  for (const el of document.querySelectorAll("[role='combobox']")) {
    if (el.querySelector("img") && (el.innerText || "").trim().length > 1) {
      if (tryClick(el)) return "combobox_with_avatar";
    }
  }
  const bar =
    document.querySelector('[role="banner"]') || document.querySelector("header") || document.body;
  let n = 0;
  for (const el of bar.querySelectorAll("[role='combobox']")) {
    n += 1;
    if (n <= 5 && (el.querySelector("img") || (el.innerText || "").trim().length > 2)) {
      if (tryClick(el)) return "header_combobox_" + n;
    }
  }
  for (const el of document.querySelectorAll("[role='combobox'],[role='button']")) {
    const al = (el.getAttribute("aria-label") || "").toLowerCase();
    if (!al) continue;
    if (al.includes("switch") || (al.includes("open") && (al.includes("menu") || al.includes("list")))) {
      if (tryClick(el)) return "aria_switch_or_menu";
    }
    if (al.includes("current") && (al.includes("page") || al.includes("asset") || al.includes("business"))) {
      if (tryClick(el)) return "aria_current";
    }
  }
  return "";
}
"""


def _scope_panel_visible(p: Page) -> bool:
    try:
        if p.evaluate(_SCOPE_OPEN_VISIBLE_JS):
            return True
    except Exception as exc:  # noqa: BLE001
        logger.debug("[MetaScan] _scope_panel_visible JS: {}", exc)
    for pat in _PLACEHOLDER_PATTERNS:
        try:
            if p.get_by_placeholder(pat).is_visible(timeout=500):
                return True
        except Exception:
            continue
    try:
        loc = p.get_by_role("combobox", name=re.compile(r"search|business|asset", re.I))
        if loc.count() and loc.first.is_visible(timeout=500):
            return True
    except Exception:
        pass
    return False


def _open_business_scope_selector(p: Page, status: StatusCallback) -> bool:
    """Mở panel lựa chọn tài sản (có ô tìm kiếm business asset) nếu chưa mở."""
    if _scope_panel_visible(p):
        return True
    status("Đang mở menu chuyển tài sản / chọn tài sản…")
    _human_delay()
    # Meta Business: combobox tên Page hiện tại (có avatar) trên thanh — bấm trước.
    try:
        reason = p.evaluate(_CLICK_ASSET_SCOPE_TRIGGER_JS)
        if reason:
            logger.debug("[MetaScan] mở scope bằng trigger JS: {}", reason)
        p.wait_for_timeout(800)
        if _scope_panel_visible(p):
            return True
    except Exception as exc:  # noqa: BLE001
        logger.debug("[MetaScan] click trigger combobox: {}", exc)
    # aria-label thường gặp (tiếng Anh).
    for label_rx in (
        re.compile(r"switch", re.I),
        re.compile(r"change.*business|business.*change", re.I),
        re.compile(r"current.*(asset|context)", re.I),
    ):
        try:
            loc = p.get_by_label(label_rx)
            if loc.count() > 0:
                loc.first.click(timeout=2_000)
                p.wait_for_timeout(500)
                if _scope_panel_visible(p):
                    return True
        except Exception:
            continue
    # Nút trên thanh/điều hướng: có thể mở scope.
    try:
        p.evaluate("""() => {
          for (const el of document.querySelectorAll('[role="button"],[role="combobox"]')) {
            const t = (el.getAttribute('aria-label') || el.innerText || '').toLowerCase();
            if (t.includes('account') && (t.includes('switch') || t.includes('select') || t.includes('open'))) {
              el.click();
              return true;
            }
          }
          return false;
        }""")
        p.wait_for_timeout(700)
        if _scope_panel_visible(p):
            return True
    except Exception as exc:  # noqa: BLE001
        logger.debug("[MetaScan] mở scope evaluate: {}", exc)
    # Click combobox/selector đầu tiên ở vùng header (kém ổn định — fallback).
    try:
        for role in ("combobox", "button"):
            loc = p.get_by_role(role)
            c = min(loc.count(), 8)
            for i in range(c):
                try:
                    loc.nth(i).click(timeout=1_200)
                    p.wait_for_timeout(400)
                    if _scope_panel_visible(p):
                        return True
                except Exception:
                    continue
    except Exception as exc:  # noqa: BLE001
        logger.debug("[MetaScan] mở scope role scan: {}", exc)
    try:
        p.wait_for_timeout(1_200)
        r2 = p.evaluate(_CLICK_ASSET_SCOPE_TRIGGER_JS)
        if r2:
            logger.debug("[MetaScan] mở scope lượt 2 (trigger): {}", r2)
        p.wait_for_timeout(1_200)
    except Exception as exc:  # noqa: BLE001
        logger.debug("[MetaScan] mở scope lượt 2: {}", exc)
    return _scope_panel_visible(p)


_SCOPE_LEFT_LIST_SCROLL_STEPS_JS = r"""
() => {
  const dialog = document.querySelector("[role=dialog]") || document.body;
  let moved = 0;
  for (const el of dialog.querySelectorAll("*")) {
    try {
      if (!el.getClientRects().length) { continue; }
      const cs = getComputedStyle(el);
      if (cs.display === "none" || cs.visibility === "hidden") { continue; }
      if (cs.overflowY !== "auto" && cs.overflowY !== "scroll") { continue; }
      if (el.scrollHeight <= el.clientHeight + 8) { continue; }
      const t = (el.innerText || "");
      if (!/\d+\s+business assets/i.test(t)) { continue; }
      if (!/Business portfolios|Your account/i.test(t)) { continue; }
      const step = Math.max(60, Math.floor(el.clientHeight * 0.8));
      const before = el.scrollTop;
      el.scrollTop = Math.min(before + step, el.scrollHeight);
      if (el.scrollTop > before) { moved += 1; }
    } catch (_) {}
  }
  return moved;
}
"""

_SCOPE_LEFT_LIST_SCROLL_TO_TOP_JS = r"""
() => {
  const dialog = document.querySelector("[role=dialog]") || document.body;
  for (const el of dialog.querySelectorAll("*")) {
    try {
      const cs = getComputedStyle(el);
      if (cs.overflowY !== "auto" && cs.overflowY !== "scroll") { continue; }
      if (el.scrollHeight <= el.clientHeight + 8) { continue; }
      const t = (el.innerText || "");
      if (!/\d+\s+business assets/i.test(t)) { continue; }
      if (!/Business portfolios|Your account/i.test(t)) { continue; }
      el.scrollTop = 0;
    } catch (_) {}
  }
  return true;
}
"""

_SCOPE_LEFT_LIST_SCROLL_TO_END_JS = r"""
() => {
  const dialog = document.querySelector("[role=dialog]") || document.body;
  for (const el of dialog.querySelectorAll("*")) {
    try {
      const cs = getComputedStyle(el);
      if (cs.overflowY !== "auto" && cs.overflowY !== "scroll") { continue; }
      if (el.scrollHeight <= el.clientHeight + 8) { continue; }
      const t = (el.innerText || "");
      if (!/\d+\s+business assets/i.test(t)) { continue; }
      if (!/Business portfolios|Your account/i.test(t)) { continue; }
      el.scrollTop = el.scrollHeight;
    } catch (_) {}
  }
  return true;
}
"""


def _scroll_left_scope_switcher_list_full(p: Page) -> None:
    """
    Cuộn cột trái (switcher) xuống tận cùng rồi hồi lên đầu — đủ cả «Business portfolios»
    và «Your account» trong DOM (list lazy/scroll nội bộ).
    """
    for _ in range(45):
        n = 0
        try:
            n = int(p.evaluate(_SCOPE_LEFT_LIST_SCROLL_STEPS_JS) or 0)
        except Exception as exc:  # noqa: BLE001
            logger.debug("[MetaScan] scroll left steps: {}", exc)
            break
        p.wait_for_timeout(100)
        if n < 1:
            try:
                p.evaluate(_SCOPE_LEFT_LIST_SCROLL_TO_END_JS)
            except Exception:
                pass
            p.wait_for_timeout(120)
            break
    p.wait_for_timeout(120)
    try:
        p.evaluate(_SCOPE_LEFT_LIST_SCROLL_TO_END_JS)
    except Exception:
        pass
    p.wait_for_timeout(150)
    try:
        p.evaluate(_SCOPE_LEFT_LIST_SCROLL_TO_TOP_JS)
    except Exception:
        pass
    p.wait_for_timeout(200)


def _scroll_left_scope_switcher_end_then_top(p: Page) -> None:
    """
    Mở lại menu sau: chỉ tới **đuôi** cột rồi về **đầu** (nhanh) — đủ bật mục lazy.
    Dùng trước từng lần click context (tránh bản cuộn từng bước dài ở mỗi mục).
    """
    for _ in range(12):
        try:
            m = int(p.evaluate(_SCOPE_LEFT_LIST_SCROLL_STEPS_JS) or 0)
        except Exception:  # noqa: BLE001
            m = 0
        p.wait_for_timeout(80)
        if m < 1:
            break
    p.wait_for_timeout(100)
    try:
        p.evaluate(_SCOPE_LEFT_LIST_SCROLL_TO_END_JS)
    except Exception:  # noqa: BLE001
        pass
    p.wait_for_timeout(120)
    try:
        p.evaluate(_SCOPE_LEFT_LIST_SCROLL_TO_TOP_JS)
    except Exception:  # noqa: BLE001
        pass
    p.wait_for_timeout(180)


def _list_portfolio_entries(p: Page) -> list[dict[str, Any]]:
    try:
        raw = p.evaluate(_LIST_PORTFOLIOS_JS)
    except Exception as exc:  # noqa: BLE001
        logger.warning("[MetaScan] list portfolio JS: {}", exc)
        return []
    if not isinstance(raw, list):
        return []
    out: list[dict[str, Any]] = []
    for x in raw:
        if not isinstance(x, dict):
            continue
        try:
            n_ = int(str(x.get("count") or "0"))
        except (TypeError, ValueError):
            continue
        if n_ < 0:
            continue
        out.append(x)
    return out


def _flex_name_pattern(name: str) -> re.Pattern[str]:
    """Khoảng trắng giữa các từ linh hoạt; tên một từ: ranh giới từ."""
    parts = [re.escape(p) for p in str(name or "").split() if p]
    if not parts:
        return re.compile(r"^$", re.I)
    if len(parts) == 1:
        s = parts[0]
        if len(s) > 2:
            return re.compile(rf"(?<!\w){s}(?!\w)", re.I)
        return re.compile(s, re.I)
    return re.compile(r"\s+".join(parts), re.I)


def _fold_ascii(s: str) -> str:
    """Chuẩn hóa so khớp tên (bỏ dấu, chữ thường)."""
    t = unicodedata.normalize("NFD", (s or "").strip().casefold())
    return "".join(c for c in t if unicodedata.category(c) != "Mn")


def _row_text_matches_business_name(name: str, row_text: str) -> bool:
    if not name or not row_text:
        return False
    fn, rt = _fold_ascii(name), _fold_ascii(row_text)
    if fn in rt:
        return True
    words = [w for w in name.replace("\n", " ").split() if len(w) >= 2]
    if not words:
        return False
    return all(_fold_ascii(w) in rt for w in words)


def _click_business_playwright(p: Page, name: str, count_s: str) -> bool:
    """
    Bấm dòng context trên **lưới cột trái** (grid có Business portfolios / Your account).
    Playwright + force thường ổn định hơn el.click() trên React ảo.
    """
    assets_re = re.compile(rf"{re.escape(count_s)}\s+business\s+assets", re.I)
    try:
        n_c = int(count_s)
    except ValueError:
        return False
    if n_c < 1:
        return False
    try:
        dialog = p.locator('[role="dialog"]').first
        if dialog.count() == 0:
            dialog = p.locator("body")
        grids = dialog.locator('[role="grid"]')
        n_grids = min(grids.count(), 12)
        left_grid = None
        for gi in range(n_grids):
            g = grids.nth(gi)
            try:
                sample = (g.inner_text(timeout=2_000) or "")[:6000]
            except Exception:  # noqa: BLE001
                continue
            if re.search(r"Business portfolios|Your account", sample, re.I):
                left_grid = g
                break
        if left_grid is None and n_grids > 0:
            left_grid = grids.first
        if left_grid is None:
            return False
        rows = left_grid.locator('[role="row"]')
        n_rows = min(rows.count(), 50)
        for ri in range(n_rows):
            row = rows.nth(ri)
            try:
                tx = row.inner_text(timeout=2_500) or ""
            except Exception:  # noqa: BLE001
                continue
            if not assets_re.search(tx):
                continue
            if not _row_text_matches_business_name(name, tx):
                continue
            cell = row.locator('[role="gridcell"]').first
            target = cell if cell.count() else row
            target.scroll_into_view_if_needed(timeout=8_000)
            p.wait_for_timeout(120)
            target.click(timeout=15_000, force=True, delay=40)
            p.wait_for_timeout(400)
            return True
    except Exception as exc:  # noqa: BLE001
        logger.debug("[MetaScan] Playwright click «{}»: {}", name, exc)
    return False


def _click_business(p: Page, name: str, count: str) -> bool:
    count_s = str(count)
    try:
        if int(count_s) < 1:
            return False
    except ValueError:
        return False
    if _click_business_playwright(p, name, count_s):
        return True
    try:
        if p.evaluate(_CLICK_BUSINESS_BY_NAME_COUNT_JS, name, count_s):
            p.wait_for_timeout(300)
            return True
    except Exception as exc:  # noqa: BLE001
        logger.debug("[MetaScan] click business JS {}: {}", name, exc)

    # Fallback: ô cột trái — "N business assets" + tên (exact, heading, hoặc regex tên mềm).
    assets_re = re.compile(rf"{re.escape(count_s)}\s+business assets", re.I)
    name_rx = _flex_name_pattern(name)
    try:
        dialog = p.locator('[role="dialog"]').first
        if dialog.count() == 0:
            dialog = p.locator("body")
        left_g = dialog.locator('[role="grid"]').filter(
            has_text=re.compile(r"Business portfolios|Your account", re.I),
        )
        if left_g.count() == 0:
            search_root = dialog
        else:
            search_root = left_g.first
        gc = search_root.locator('[role="gridcell"], [role="row"]').filter(has_text=assets_re)
        name_in = dialog.get_by_text(name, exact=True)
        row = gc.filter(has=name_in)
        if row.count() == 0:
            row = gc.filter(
                has=dialog.get_by_role("heading", name=name, exact=True),
            )
        if row.count() == 0:
            row = gc.filter(has=dialog.get_by_text(name_rx))
        if row.count() == 0:
            row = dialog.get_by_role("button", name=name, exact=True).filter(has_text=assets_re)
        if row.count() == 0:
            b = dialog.get_by_text(assets_re)
            if b.count() > 0:
                row = b.filter(has=dialog.get_by_text(name_rx))
        if row.count():
            c = row.first.locator('[role="gridcell"]').first
            if c.count() == 0:
                c = row.first
            c.scroll_into_view_if_needed(timeout=5_000)
            c.click(timeout=12_000, force=True)
            p.wait_for_timeout(300)
            return True
    except Exception as exc:  # noqa: BLE001
        logger.debug("[MetaScan] click business fallback {}: {}", name, exc)
    return False


def _view_more_loop(p: Page, status: StatusCallback, max_clicks: int = 500) -> None:
    for i in range(max_clicks):
        btn = p.get_by_role("button", name=re.compile(r"^View more$", re.I))
        if btn.count() == 0:
            try:
                alt = p.get_by_text("View more", exact=True)
            except Exception:
                alt = None
            if alt and alt.is_visible():
                try:
                    alt.click(timeout=2_000)
                except Exception:
                    break
                p.wait_for_timeout(450)
                _human_delay()
                status(f"View more ({i + 1})…")
                continue
            break
        try:
            if not btn.first.is_visible():
                break
        except Exception:
            break
        try:
            btn.first.click(timeout=2_000)
        except Exception:
            break
        p.wait_for_timeout(450)
        _human_delay()
        status(f"View more ({i + 1})…")


def scan_meta_business_pages_for_account(
    context: BrowserContext,
    *,
    account_id: str = "",
    account: dict | None = None,
    start_url: str = "https://business.facebook.com/latest/home",
    status_cb: StatusCallback | None = None,
) -> list[ScannedPage]:
    """
    Quét Page qua **Meta Business Suite** — ủy quyền tới
    :func:`mbs_page_scanner.run_mbs_full_scan` (lưới ``data-surface`` + cuộn panel phải, ổn định).

    Truyền ``account`` (bản ghi từ ``accounts.json``) để validate ``browser_exe_path`` /
    ``profile_path``; ``account_id`` gắn vào từng bản ghi.
    """
    from src.automation.mbs_page_scanner import run_mbs_full_scan

    return run_mbs_full_scan(
        context,
        account,
        account_id=account_id,
        start_url=start_url,
        status_cb=status_cb,
    )
