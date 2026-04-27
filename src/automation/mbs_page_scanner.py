"""
Quét Page từ Meta Business Suite (một luồng, dễ bảo trì).

* Validate account, mở ``business.facebook.com``, mở panel chuyển tài sản.
* Với mỗi context: mục tiêu số Page = **N** từ panel phải (tiêu đề «N business assets» cạnh lưới
  — ví dụ ``<span>37 business assets</span>``) nếu đọc được; không thì dùng số ở cột trái. Sau đó
  View more + cuộn tới khi **đủ N** (hoặc hết vòng / báo thiếu).
* Thu **Page từ ``data-surface``** (``business_scope:page:<id>:…``) qua
  ``meta_business_scanner._ordered_page_rows_from_scope`` — ổn định hơn quét thẻ/card DOM
  hay parse mạng rải rác.
* Chống trùng theo ``fb_page_id``; URL dạng ``https://www.facebook.com/<id>``.

Không dùng Graph API / OAuth.
"""

from __future__ import annotations

import random
import re
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from loguru import logger
from playwright.sync_api import BrowserContext, Page, TimeoutError as PlaywrightTimeoutError

from src.automation.facebook_page_scanner import ScannedPage
from src.utils.screenshot import capture_page_screenshot

StatusCallback = Callable[[str], None]

_STABLE_ROUNDS_MAX = 5
# Vòng ổn định trước khi từ bỏ nếu vẫn thiếu so với số tài sản mục tiêu
_STABLE_INCOMPLETE_GIVEUP = 12


def _noop_status(_msg: str) -> None:
    pass


def validate_account_for_mbs_scan(account: dict[str, Any] | None) -> list[str]:
    """
    Kiểm tra tài khoản trước khi quét: active, ``browser_exe_path``, ``profile_path``, proxy nếu có.
    Trả về danh sách lỗi (rỗng = ok).
    """
    errs: list[str] = []
    if not account:
        return []  # không bắt buộc dict khi tương thích API cũ; không kiểm tra path
    st = str(account.get("status") or "active").strip().lower()
    if st and st not in ("active", "ok", "enabled", "1", "true"):
        errs.append(f"Trạng thái tài khoản không phù hợp: {st!r}")

    bexe = str(account.get("browser_exe_path") or "").strip()
    if bexe:
        p = Path(bexe)
        if not p.is_file():
            errs.append(f"Không tìm thấy file trình duyệt: {bexe}")

    prof = str(account.get("profile_path") or "").strip()
    if prof:
        pp = Path(prof)
        if not pp.is_dir():
            errs.append(f"Không tìm thấy thư mục profile: {prof}")

    if account.get("use_proxy", True):
        px = account.get("proxy") or {}
        host = str((px or {}).get("host") or "").strip()
        try:
            port = int((px or {}).get("port") or 0)
        except (TypeError, ValueError):
            port = 0
        if host and port <= 0:
            errs.append("Proxy: có host nhưng port không hợp lệ.")

    return errs


_FIND_RIGHT_SCROLL_JS = r"""
() => {
  const dialog = document.querySelector('[role="dialog"]') || document.body;
  let best = null;
  let bestScore = 0;
  for (const el of dialog.querySelectorAll('div, section, main')) {
    try {
      const sh = el.scrollHeight || 0;
      const ch = el.clientHeight || 0;
      if (sh <= ch + 8) { continue; }
      const t = (el.innerText || '');
      if (!t.includes('Facebook Page')) { continue; }
      const score = sh * (t.split('\n').length);
      if (score > bestScore) { bestScore = score; best = el; }
    } catch (_) {}
  }
  if (!best) { return false; }
  window.__mbsRightListScroll = best;
  try { best.scrollTop = 0; } catch (_) {}
  return true;
}
"""

_SCROLL_ASSIGNED_JS = r"""
(step) => {
  const el = window.__mbsRightListScroll;
  if (!el) { return false; }
  try {
    el.scrollTop = Math.min(el.scrollTop + step, el.scrollHeight);
    return true;
  } catch (_) { return false; }
}
"""

# Đọc số tài sản từ **cột phải** (header trùng «N business assets» — không dựa class xoáy Meta).
_READ_RIGHT_ASSETS_COUNT_JS = r"""
() => {
  const re = /(\d+)\s+business assets/i;
  const onlyText = (s) => String(s || "").replace(/\r/g, "").replace(/\n/g, " ").replace(/\s+/g, " ").trim();
  const onlyLine = /^\d+\s+business assets$/i;
  const dialog = document.querySelector('[role="dialog"]') || document.body;
  const drect = dialog.getBoundingClientRect();
  if (drect.width < 8) { return null; }
  const midX = drect.left + drect.width * 0.42;

  // 1) Từ vùng cuộn Page đã bắt: đi lên cây DOM, lấy số ở phần đầu nội dung.
  const sc = window.__mbsRightListScroll;
  if (sc) {
    for (let p = sc, u = 0; u < 10 && p; u++, p = p.parentElement) {
      const t = p.innerText || "";
      if (!t.includes("Facebook Page") || !re.test(t)) { continue; }
      const m = t.substring(0, 1400).match(re);
      if (m) { return parseInt(m[1], 10); }
    }
  }

  // 2) Nút/span gần như chỉ là «N business assets» nằm bên phải dialog (cột dữ liệu, không cột trái).
  let bestN = 0;
  let bestLeft = -1;
  for (const el of dialog.querySelectorAll("span, div, p, a, label")) {
    const raw = onlyText(el.textContent);
    if (!raw || !onlyLine.test(raw)) { continue; }
    if (raw.length > 40) { continue; }
    const r = el.getBoundingClientRect();
    if (r.width < 2 || r.height < 2) { continue; }
    if (r.left < midX) { continue; }
    const m = raw.match(onlyLine);
    if (m) {
      const n = parseInt(m[1], 10);
      if (n > 0 && r.left >= bestLeft) {
        bestLeft = r.left;
        bestN = n;
      }
    }
  }
  if (bestN) { return bestN; }

  // 3) Khối có nhiều dòng «Facebook Page» (danh sách cột phải): số ở ~đầu khối.
  for (const el of dialog.querySelectorAll("div, section, main, ul, ol, [role='list']")) {
    const t = el.innerText || "";
    if ((t.match(/Facebook Page/g) || []).length < 2) { continue; }
    if (!re.test(t)) { continue; }
    const m = t.substring(0, 900).match(re);
    if (m) { return parseInt(m[1], 10); }
  }
  return null;
}
"""


def _read_right_panel_business_asset_count(page: Page) -> int | None:
    """
    Số tài sản **hiện tại** trên panel phải (header «N business assets»), ưu tiên so với cột trái.
    """
    try:
        n = page.evaluate(_READ_RIGHT_ASSETS_COUNT_JS)
    except Exception as exc:  # noqa: BLE001
        logger.debug("[Mbs] read right asset count: {}", exc)
        return None
    if n is None:
        return None
    try:
        v = int(n)
    except (TypeError, ValueError):
        return None
    return v if v > 0 else None


def _find_right_scroll_container(page: Page) -> bool:
    try:
        return bool(page.evaluate(_FIND_RIGHT_SCROLL_JS))
    except Exception as exc:  # noqa: BLE001
        logger.warning("[MbsScroll] Không gán được container phải: {}", exc)
        return False


def _scroll_right_panel(page: Page, step_px: int = 900) -> bool:
    try:
        return bool(page.evaluate(_SCROLL_ASSIGNED_JS, step_px))
    except Exception:
        return False


def _public_facebook_url(pid: str) -> str:
    p = str(pid or "").strip()
    return f"https://www.facebook.com/{p}" if p else ""


def _merge_data_surface_rows(
    page: Page,
    store: dict[str, ScannedPage],
    mbs_mod: Any,
    *,
    business_name: str,
    business_id: str,
    account_id: str,
    source: str,
) -> int:
    """Gộp Page từ lưới phải (thuộc tính ``data-surface`` / ``_ORDERED_PAGE_SCAN_JS``)."""
    rows = mbs_mod._ordered_page_rows_from_scope(page)  # type: ignore[attr-defined]
    before = len(store)
    for row in rows:
        if not isinstance(row, dict):
            continue
        pid = str((row or {}).get("id") or "").strip()
        if not pid.isdigit() or len(pid) < 6:
            continue
        name = str((row or {}).get("name") or "").strip() or pid
        if pid in store:
            cur = dict(store[pid])
            pn = (cur.get("page_name") or "").strip()
            if name and (not pn or len(name) > len(pn)):
                cur["page_name"] = name
            store[pid] = ScannedPage(**cur)  # type: ignore[arg-type]
            continue
        store[pid] = ScannedPage(
            page_name=name,
            page_url=_public_facebook_url(pid),
            fb_page_id=pid,
            page_kind="fanpage",
            source=source,
            business_name=business_name,
            business_id=business_id or "",
            account_id=account_id,
            role="unknown",
        )
    return len(store) - before


def _view_more_once(page: Page, status: StatusCallback) -> bool:
    try:
        btn = page.get_by_role("button", name=re.compile(r"^View more$", re.I))
        if btn.count() and btn.first.is_visible():
            btn.first.click(timeout=2_000)
            page.wait_for_timeout(800)
            status("Đã bấm View more (1 lần).")
            return True
    except Exception:
        pass
    try:
        alt = page.get_by_text("View more", exact=True)
        if alt.is_visible():
            alt.click(timeout=1_500)
            page.wait_for_timeout(800)
            status("Đã bấm View more (text).")
            return True
    except Exception:
        pass
    return False


def _stable_scan_context(
    page: Page,
    *,
    business_name: str,
    business_id: str,
    account_id: str,
    mbs_mod: Any,
    status: StatusCallback,
    expected_page_total: int | None = None,
) -> dict[str, ScannedPage]:
    """
    Cuộn panel phải + (thỉnh thoảng) View more; gộp từ ``data-surface``.

    Nếu ``expected_page_total`` (số từ dòng vừa chọn, ví dụ «37 business assets») được truyền,
    ưu tiên dừng khi đã thu **đủ** số Page; chỉ dùng “ổn định không tăng” khi **không** biết
    mục tiêu hoặc khi mục tiêu đã đạt mà thêm cũng được.
    """
    per: dict[str, ScannedPage] = {}
    label_base = f"mbs|{(account_id or '')[:8]}"
    if expected_page_total and expected_page_total > 0:
        status(
            f"Mục tiêu: {expected_page_total} Page (theo số tài sản bên cột trái) — «{business_name}».",
        )
    if not _find_right_scroll_container(page):
        logger.warning("[Mbs] Không gắn được container cuộn phải — vẫn thu từ data-surface nếu có.")
        try:
            capture_page_screenshot(page, f"mbs_khong_scroll_{int(time.time())}")
        except Exception as exc:  # noqa: BLE001
            logger.debug("screenshot: {}", exc)
    else:
        logger.info("[Mbs] Đã gắn __mbsRightListScroll cho panel phải.")

    stable = 0
    last_count = -1
    exp = expected_page_total if (expected_page_total and expected_page_total > 0) else None
    # Đủ vòng khi list dài; có mục tiêu thì cho thêm lượt cuộn
    max_rounds = min(120, max(32, (exp * 2 + 20) if exp else 32))
    for round_i in range(max_rounds):
        added = _merge_data_surface_rows(
            page,
            per,
            mbs_mod,
            business_name=business_name,
            business_id=business_id,
            account_id=account_id,
            source=f"{label_base}|r{round_i + 1}",
        )
        cur = len(per)
        line = f"Vòng {round_i + 1}: +{added} Page mới, tổng {cur}"
        if exp:
            line += f" / {exp}"
        line += f" (context «{business_name}»)."
        status(line)

        if exp is not None and cur >= exp:
            status(
                f"Đã thu đủ {cur}/{exp} Page (khớp số tài sản) — kết thúc «{business_name}».",
            )
            break

        if cur == last_count and added == 0:
            stable += 1
            if exp is None:
                if stable >= _STABLE_ROUNDS_MAX:
                    status(
                        f"Ổn định: {stable} vòng không tăng — kết thúc context «{business_name}».",
                    )
                    break
            else:
                # Còn thiếu so với mục tiêu: thử bật nội dung / cuộn mạnh trước khi bỏ cuộc
                if stable in (4, 8):
                    _view_more_once(page, status)
                    try:
                        page.evaluate(
                            """() => { const d=document.querySelector('[role=dialog]')||document.body;
      const a=[...d.querySelectorAll('*')].filter(
        e=>(getComputedStyle(e).overflowY==='auto'||getComputedStyle(e).overflowY==='scroll')
        && e.scrollHeight > e.clientHeight+10);
      for (const el of a) { try { el.scrollTop = el.scrollHeight; } catch (_) {} } }""",
                        )
                    except Exception:
                        pass
                if stable >= _STABLE_INCOMPLETE_GIVEUP:
                    status(
                        f"Cảnh báo: mới thu {cur}/{exp} Page — dừng (thiếu, có thể do UI) «{business_name}».",
                    )
                    break
        else:
            stable = 0
        last_count = cur

        if round_i % 3 == 0:
            _view_more_once(page, status)
        if not _scroll_right_panel(page, random.randint(700, 1_200)):
            try:
                page.evaluate(
                    """() => { const d=document.querySelector('[role=dialog]')||document.body;
                    const a=[...d.querySelectorAll('*')].filter(
                      e=>(getComputedStyle(e).overflowY==='auto'||getComputedStyle(e).overflowY==='scroll')
                      && e.scrollHeight > e.clientHeight+10);
                    if (a[0]) a[0].scrollTop += 900; }""",
                )
            except Exception:
                pass
        time.sleep(random.uniform(0.4, 0.8))
    if exp is not None and len(per) < exp:
        status(
            f"Hết số vòng quét: thu {len(per)}/{exp} Page (chưa đủ) — «{business_name}».",
        )
    return per


def run_mbs_full_scan(
    context: BrowserContext,
    account: dict[str, Any] | None,
    *,
    account_id: str = "",
    start_url: str = "https://business.facebook.com/latest/home",
    status_cb: StatusCallback | None = None,
) -> list[ScannedPage]:
    """
    Từng mục business cột trái: View more, rồi thu Page từ lưới ``data-surface`` (panel phải) cho đến ổn định.
    """
    status = status_cb or _noop_status
    errs = validate_account_for_mbs_scan(account)
    if errs:
        for e in errs:
            logger.error("[Mbs] Validate account: {}", e)
        status("Lỗi cấu hình account: " + "; ".join(errs))
        return []

    # Import vòng tránh vòng lặp import
    from src.automation import meta_business_scanner as mbs_mod

    aid = (account_id or str((account or {}).get("id") or "")).strip()
    if account:
        logger.info("[Mbs] Bắt đầu quét Page từ tài khoản: {} ({})", aid, account.get("name", ""))
    else:
        logger.info("[Mbs] Bắt đầu quét Page (chưa có account dict) account_id={!r}.", aid)

    page = context.pages[0] if context.pages else context.new_page()
    global_all: dict[str, ScannedPage] = {}

    def _merge_ctx_into_global(per_ctx: dict[str, ScannedPage]) -> None:
        for k, v in per_ctx.items():
            v2: dict[str, Any] = dict(v)
            v2["account_id"] = aid
            v2["role"] = (v2.get("role") or "").strip() or "unknown"
            rid = str(v2.get("fb_page_id") or k or "").strip()
            if not rid:
                continue
            if rid in global_all:
                old = dict(global_all[rid])
                for f2, val2 in v2.items():
                    if val2 in (None, ""):
                        continue
                    o_v = old.get(f2) if f2 in old else None
                    if o_v in (None, ""):
                        old[f2] = val2
                global_all[rid] = ScannedPage(**old)  # type: ignore[arg-type]
            else:
                global_all[rid] = ScannedPage(**v2)  # type: ignore[arg-type]

    try:
        page.goto(start_url, wait_until="domcontentloaded", timeout=90_000)
    except Exception as exc:  # noqa: BLE001
        logger.error("[Mbs] Không mở Meta Business: {}", exc)
        capture_page_screenshot(page, f"mbs_goto_loi_{int(time.time())}")
        status(f"Lỗi mở Meta Business Suite: {exc}")
        return []
    try:
        page.wait_for_load_state("networkidle", timeout=12_000)
    except PlaywrightTimeoutError:
        pass
    page.wait_for_timeout(1_000)

    if not mbs_mod._open_business_scope_selector(page, status):  # type: ignore[attr-defined]
        logger.error("[Mbs] Không mở được panel chuyển tài sản.")
        capture_page_screenshot(page, f"mbs_khong_mo_scope_{int(time.time())}")
        status("Không mở được menu tài sản — dừng.")
        return []

    status("Cuộn cột trái để tải đủ cả «Business portfolios» và «Your account»…")
    mbs_mod._scroll_left_scope_switcher_list_full(page)  # type: ignore[attr-defined]
    entries = mbs_mod._list_portfolio_entries(page)  # type: ignore[attr-defined]
    if not entries:
        logger.warning("[Mbs] Không có mục context trái (N business assets) — fallback quét context hiện tại.")
        capture_page_screenshot(page, f"mbs_khong_context_{int(time.time())}")
        status("Không đọc được danh sách cột trái, thử quét trực tiếp context hiện tại (Your account).")
        mbs_mod._view_more_loop(page, status, max_clicks=80)  # type: ignore[attr-defined]
        _find_right_scroll_container(page)
        n_from_right = _read_right_panel_business_asset_count(page)
        if n_from_right is not None:
            status(f"Fallback panel phải: {n_from_right} business assets.")
        bname_fallback = str((account or {}).get("name") or "").strip() or "Your account"
        per_ctx = _stable_scan_context(
            page,
            business_name=bname_fallback,
            business_id="",
            account_id=aid,
            mbs_mod=mbs_mod,
            status=status,
            expected_page_total=n_from_right,
        )
        _merge_ctx_into_global(per_ctx)
        out = [global_all[k] for k in sorted(global_all.keys(), key=lambda x: (x.startswith("__"), x))]
        logger.info("[Mbs] Fallback hoàn tất: {} Page (context hiện tại).", len(out))
        status(f"SUCCESS: Fallback quét được {len(out)} Page (account_id={aid!r}).")
        return out
    status(f"Đã phát hiện {len(entries)} mục context bên trái.")

    for j, ent in enumerate(entries):
        bname = str(ent.get("name") or "").strip()
        bcount = str(ent.get("count") or "").strip()
        bid = str(ent.get("business_id") or "").strip()
        if not bname or not bcount:
            continue
        try:
            n_assets = int(bcount)
        except (TypeError, ValueError):
            n_assets = -1
        if n_assets == 0:
            status(f"Bỏ qua «{bname}»: 0 business assets (không có Page để quét).")
            logger.info("[Mbs] Bỏ qua context 0 tài sản: «{}»", bname)
            continue
        if n_assets < 0:
            continue
        status(f"Đang quét context: «{bname}» ({j + 1}/{len(entries)}) — {bcount} tài sản…")
        mbs_mod._human_delay()  # type: ignore[attr-defined]
        if not mbs_mod._open_business_scope_selector(page, status):  # type: ignore[attr-defined]
            logger.warning("[Mbs] Không mở lại panel tài sản trước context: {}", bname)
        mbs_mod._human_delay()  # type: ignore[attr-defined]
        # Không cuộn end→top trước click: dễ làm list ảo Meta bỏ mount dòng giữa; Playwright tự scrollIntoView khi bấm.
        clicked = mbs_mod._click_business(page, bname, bcount)  # type: ignore[attr-defined]
        if not clicked:
            status(f"Thử lại sau khi cuộn cột trái: «{bname}»…")
            mbs_mod._scroll_left_scope_switcher_end_then_top(page)  # type: ignore[attr-defined]
            mbs_mod._human_delay()  # type: ignore[attr-defined]
            clicked = mbs_mod._click_business(page, bname, bcount)  # type: ignore[attr-defined]
        if not clicked:
            logger.warning("[Mbs] Không click được context: {}", bname)
            continue
        page.wait_for_timeout(1_000)
        try:
            page.wait_for_load_state("domcontentloaded", timeout=8_000)
        except PlaywrightTimeoutError:
            pass
        mbs_mod._view_more_loop(page, status, max_clicks=80)  # type: ignore[attr-defined]

        _find_right_scroll_container(page)
        n_from_right = _read_right_panel_business_asset_count(page)

        try:
            target_n = int(bcount)
        except (TypeError, ValueError):
            target_n = None
        else:
            if target_n < 1:
                target_n = None

        if n_from_right is not None:
            if target_n is not None and target_n != n_from_right:
                logger.info(
                    "[Mbs] Số tài sản cột phải ({}) khác cột trái ({}) — ưu tiên cột phải.",
                    n_from_right,
                    target_n,
                )
            target_n = n_from_right
            status(f"Panel phải: {n_from_right} business assets (mục tiêu thu).")
        elif target_n is not None:
            status(f"Mục tiêu từ cột trái: {target_n} (không đọc được số ở panel phải).")

        per_ctx = _stable_scan_context(
            page,
            business_name=bname,
            business_id=bid,
            account_id=aid,
            mbs_mod=mbs_mod,
            status=status,
            expected_page_total=target_n,
        )
        _merge_ctx_into_global(per_ctx)
        if not mbs_mod._scope_panel_visible(page):  # type: ignore[attr-defined]
            mbs_mod._open_business_scope_selector(page, status)  # type: ignore[attr-defined]
        page.wait_for_timeout(500)

    out = [global_all[k] for k in sorted(global_all.keys(), key=lambda x: (x.startswith("__"), x))]
    logger.info("[Mbs] Hoàn tất: {} Page (hợp nhất từ data-surface).", len(out))
    status(f"SUCCESS: Đã quét được {len(out)} Page (account_id={aid!r}).")
    return out


# alias cho import
__all__ = (
    "run_mbs_full_scan",
    "validate_account_for_mbs_scan",
)
