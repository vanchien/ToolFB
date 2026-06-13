"""
HumanAction — bọc Playwright với chuột Bezier, con trỏ ảo, cuộn/click/gõ giống người.

Thay ``page.click(selector)`` bằng ``HumanAction(page).smart_click(selector)``.
"""

from __future__ import annotations

import math
import os
import random
import time
import weakref
from typing import Any

from loguru import logger
from playwright.sync_api import BrowserContext, Locator, Page, TimeoutError as PlaywrightTimeoutError

from src.utils.delays import random_ms

# Vị trí chuột ảo theo từng Page (tránh nhảy từ góc 0,0 mỗi lần click).
_MOUSE_STATE: weakref.WeakKeyDictionary[Page, dict[str, float]] = weakref.WeakKeyDictionary()
_CONTEXT_CURSOR_INIT: weakref.WeakKeyDictionary[BrowserContext, bool] = weakref.WeakKeyDictionary()
_PAGE_NAV_HOOK: weakref.WeakKeyDictionary[Page, bool] = weakref.WeakKeyDictionary()

# Con trỏ ảo — 2 lớp SVG (mũi tên / bàn tay), transform GPU, không nhấp nháy.
_CURSOR_JS = r"""
(() => {
  if (window.__toolfbCursor && window.__toolfbCursor.ready) return;
  const ST = window.__toolfbCursorState || { x: -200, y: -200, mode: 'default', visible: false };
  window.__toolfbCursorState = ST;
  const SH = 'filter:drop-shadow(-2px 3px 1.5px rgba(0,0,0,.22));';
  const mk = (path, w, h, vb) =>
    '<svg xmlns="http://www.w3.org/2000/svg" width="' + w + '" height="' + h + '" viewBox="' + vb + '" style="' + SH + 'display:block">'
    + '<path fill="#fff" stroke="#111" stroke-width="1" stroke-linejoin="round" stroke-linecap="round" d="' + path + '"/></svg>';
  const ARROW_PATH = 'M1 1 L1 18.5 L7 13 L10.5 21 L13.5 19.5 L9.5 12 L19 12 L1 1 Z';
  const HAND_PATH = 'M11 2.5c-.55 0-1 .45-1 1V9H9c-.83 0-1.5.67-1.5 1.5V12c0 .45.2.86.52 1.15-.72.24-1.22.94-1.22 1.75V19c0 2.2 1.8 4 4 4h5.2c2.65 0 4.8-2.15 4.8-4.8V10.2c0-.55-.45-1-1-1s-1 .45-1 1V7.5c0-.55-.45-1-1-1s-1 .45-1 1V5c0-.55-.45-1-1-1s-1 .45-1 1V3.5c0-.55-.45-1-1-1z';
  let root = document.getElementById('__toolfb_human_cursor');
  if (!root) {
    root = document.createElement('div');
    root.id = '__toolfb_human_cursor';
    root.style.cssText = 'position:fixed;left:0;top:0;z-index:2147483647;pointer-events:none;will-change:transform;opacity:0;';
    const arrow = document.createElement('div');
    arrow.id = '__toolfb_cursor_arrow';
    arrow.innerHTML = mk(ARROW_PATH, 20, 20, '0 0 20 20');
    arrow.style.cssText = 'position:absolute;left:0;top:0;';
    const hand = document.createElement('div');
    hand.id = '__toolfb_cursor_hand';
    hand.innerHTML = mk(HAND_PATH, 28, 28, '0 0 28 28');
    hand.style.cssText = 'position:absolute;left:0;top:0;display:none;';
    root.appendChild(arrow);
    root.appendChild(hand);
    (document.documentElement || document.body).appendChild(root);
  }
  const arrowEl = document.getElementById('__toolfb_cursor_arrow');
  const handEl = document.getElementById('__toolfb_cursor_hand');
  const apply = () => {
    const ptr = ST.mode === 'pointer' || ST.mode === 'click' || ST.mode === 'hand';
    if (arrowEl) arrowEl.style.display = ptr ? 'none' : 'block';
    if (handEl) handEl.style.display = ptr ? 'block' : 'none';
    const ox = ptr ? 10 : 0;
    const oy = ptr ? 3 : 0;
    root.style.opacity = ST.visible ? '1' : '0';
    root.style.transform = 'translate3d(' + (ST.x - ox) + 'px,' + (ST.y - oy) + 'px,0)';
  };
  window.__toolfbMoveCursor = (x, y, mode) => {
    if (typeof x === 'number' && !isNaN(x)) ST.x = x;
    if (typeof y === 'number' && !isNaN(y)) ST.y = y;
    if (mode) ST.mode = (mode === 'click' || mode === 'hand') ? 'pointer' : mode;
    ST.visible = true;
    apply();
  };
  window.__toolfbMoveCursorBatch = (points, finalMode) => {
    if (Array.isArray(points) && points.length) {
      const last = points[points.length - 1];
      ST.x = last[0]; ST.y = last[1];
    }
    if (finalMode) ST.mode = finalMode === 'click' ? 'pointer' : finalMode;
    ST.visible = true;
    apply();
  };
  apply();
  window.__toolfbCursor = { ready: true, apply: apply };
})();
"""

_CURSOR_INIT_SCRIPT = _CURSOR_JS

_CURSOR_MOVE_INLINE = """([x, y, mode]) => {
  if (typeof window.__toolfbMoveCursor === 'function') {
    window.__toolfbMoveCursor(x, y, mode || null);
  }
}"""

_CURSOR_BATCH_INLINE = """([points, mode]) => {
  if (typeof window.__toolfbMoveCursorBatch === 'function') {
    window.__toolfbMoveCursorBatch(points, mode || 'default');
  } else if (typeof window.__toolfbMoveCursor === 'function' && points && points.length) {
    const p = points[points.length - 1];
    window.__toolfbMoveCursor(p[0], p[1], mode || 'default');
  }
}"""


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name, "").strip().lower()
    if raw in ("1", "true", "yes", "on"):
        return True
    if raw in ("0", "false", "no", "off"):
        return False
    return default


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(float(raw))
    except (TypeError, ValueError):
        return default


def install_virtual_cursor_on_context(context: BrowserContext) -> None:
    """
    Gắn con trỏ ảo lên mọi trang — ``add_init_script`` tự chạy sau navigate (không re-init gây nhấp nháy).
    """
    if _CONTEXT_CURSOR_INIT.get(context):
        return
    try:
        context.add_init_script(_CURSOR_INIT_SCRIPT)
        _CONTEXT_CURSOR_INIT[context] = True
        for pg in context.pages:
            _hook_page_cursor_refresh(pg)
            _ensure_cursor_on_page(pg, sync_pos_from_state=True)

        def _on_new_page(page: Page) -> None:
            _hook_page_cursor_refresh(page)
            _ensure_cursor_on_page(page, sync_pos_from_state=True)

        try:
            context.on("page", _on_new_page)
        except Exception:
            pass
        logger.debug("[HumanAction] Con trỏ ảo init-script trên context ({} trang).", len(context.pages))
    except Exception as exc:  # noqa: BLE001
        logger.debug("[HumanAction] install_virtual_cursor_on_context: {}", exc)


def _ensure_cursor_on_page(page: Page, *, sync_pos_from_state: bool = False) -> None:
    """Gắn con trỏ nếu thiếu; tuỳ chọn đồng bộ vị trí từ state Python."""
    try:
        page.evaluate(_CURSOR_INIT_SCRIPT)
        if sync_pos_from_state:
            st = _MOUSE_STATE.get(page)
            if st:
                page.evaluate(_CURSOR_MOVE_INLINE, [float(st["x"]), float(st["y"]), "default"])
    except Exception:
        pass


def _hook_page_cursor_refresh(page: Page) -> None:
    """Sau navigate SPA: chỉ khôi phục vị trí, không tạo lại DOM."""
    if _PAGE_NAV_HOOK.get(page):
        return

    def _on_nav(frame: Any) -> None:
        if frame != page.main_frame:
            return
        try:
            st = _MOUSE_STATE.get(page)
            if st and page.evaluate("() => !!window.__toolfbMoveCursor"):
                page.evaluate(
                    """([x,y,m]) => { window.__toolfbMoveCursor(x,y,m); }""",
                    [float(st["x"]), float(st["y"]), str(st.get("mode") or "default")],
                )
            else:
                _ensure_cursor_on_page(page, sync_pos_from_state=True)
        except Exception:
            pass

    try:
        page.on("framenavigated", _on_nav)
        _PAGE_NAV_HOOK[page] = True
    except Exception:
        pass


def _cubic_bezier(
    p0: tuple[float, float],
    p1: tuple[float, float],
    p2: tuple[float, float],
    p3: tuple[float, float],
    t: float,
) -> tuple[float, float]:
    u = 1.0 - t
    x = u**3 * p0[0] + 3 * u**2 * t * p1[0] + 3 * u * t**2 * p2[0] + t**3 * p3[0]
    y = u**3 * p0[1] + 3 * u**2 * t * p1[1] + 3 * u * t**2 * p2[1] + t**3 * p3[1]
    return x, y


def _bezier_points(
    start: tuple[float, float],
    end: tuple[float, float],
    *,
    steps: int | None = None,
) -> list[tuple[float, float]]:
    """Tạo đường cong Bezier với control points ngẫu nhiên."""
    sx, sy = start
    ex, ey = end
    dist = math.hypot(ex - sx, ey - sy)
    n = steps if steps is not None else max(12, min(48, int(dist / 18)))
    spread = max(40.0, dist * 0.35)
    c1 = (
        sx + (ex - sx) * random.uniform(0.15, 0.45) + random.uniform(-spread, spread),
        sy + (ey - sy) * random.uniform(0.05, 0.35) + random.uniform(-spread, spread),
    )
    c2 = (
        sx + (ex - sx) * random.uniform(0.55, 0.85) + random.uniform(-spread * 0.6, spread * 0.6),
        sy + (ey - sy) * random.uniform(0.65, 0.95) + random.uniform(-spread * 0.6, spread * 0.6),
    )
    pts: list[tuple[float, float]] = []
    for i in range(n + 1):
        t = i / n
        te = t * t * (3 - 2 * t)
        pts.append(_cubic_bezier(start, c1, c2, end, te))
    return pts


class HumanAction:
    """API thống nhất cho tương tác chống phát hiện bot trên Facebook."""

    def __init__(self, page: Page, *, show_cursor: bool | None = None) -> None:
        self.page = page
        self.show_cursor = _env_bool("FB_HUMAN_VIRTUAL_CURSOR", True) if show_cursor is None else show_cursor
        if page not in _MOUSE_STATE:
            vp = page.viewport_size or {"width": 1280, "height": 720}
            _MOUSE_STATE[page] = {
                "x": float(vp["width"]) * random.uniform(0.35, 0.65),
                "y": float(vp["height"]) * random.uniform(0.25, 0.55),
                "mode": "default",
            }
        if self.show_cursor:
            try:
                install_virtual_cursor_on_context(page.context)
            except Exception:
                pass
            _hook_page_cursor_refresh(page)
            _ensure_cursor_on_page(page, sync_pos_from_state=True)

    def _pos(self) -> tuple[float, float]:
        st = _MOUSE_STATE[self.page]
        return float(st["x"]), float(st["y"])

    def _set_pos(self, x: float, y: float, *, mode: str | None = None) -> None:
        st = _MOUSE_STATE[self.page]
        st["x"] = x
        st["y"] = y
        if mode is not None:
            st["mode"] = mode

    def _update_virtual_cursor(self, x: float, y: float, *, mode: str | None = None) -> None:
        if not self.show_cursor:
            return
        m = mode if mode is not None else str(_MOUSE_STATE.get(self.page, {}).get("mode") or "default")
        self._set_pos(x, y, mode=m)
        try:
            self.page.evaluate(_CURSOR_MOVE_INLINE, [float(x), float(y), m if mode else None])
        except Exception:
            _ensure_cursor_on_page(self.page, sync_pos_from_state=True)

    def move_to(self, x: float, y: float, *, steps: int | None = None, cursor_mode: str = "default") -> None:
        """Di chuyển chuột theo Bezier tới (x, y)."""
        start = self._pos()
        end = (float(x), float(y))
        if math.hypot(end[0] - start[0], end[1] - start[1]) < 4:
            self._set_pos(*end, mode=cursor_mode)
            self._update_virtual_cursor(*end, mode=cursor_mode)
            return
        path = _bezier_points(start, end, steps=steps)
        mouse = self.page.mouse
        last_i = len(path) - 1
        update_stride = max(1, _env_int("FB_CURSOR_UPDATE_EVERY", 2))
        for i, (px, py) in enumerate(path):
            mouse.move(px, py)
            step_mode = cursor_mode if i == last_i else "default"
            if self.show_cursor and (i == last_i or i % update_stride == 0):
                self._update_virtual_cursor(px, py, mode=step_mode)
            if i < last_i:
                time.sleep(random.uniform(0.005, 0.018))
        self._set_pos(*end, mode=cursor_mode)

    def move_to_locator(self, locator: Locator, *, jitter: float = 4.0, pointer_on_arrive: bool = True) -> tuple[float, float]:
        """Di chuyển tới tâm phần tử (có jitter nhỏ)."""
        box = locator.bounding_box()
        if not box:
            locator.scroll_into_view_if_needed(timeout=5_000)
            box = locator.bounding_box()
        if not box:
            raise PlaywrightTimeoutError("Không đọc được bounding box phần tử.")
        tx = box["x"] + box["width"] * random.uniform(0.35, 0.65) + random.uniform(-jitter, jitter)
        ty = box["y"] + box["height"] * random.uniform(0.35, 0.65) + random.uniform(-jitter, jitter)
        arrive_mode = "pointer" if pointer_on_arrive else "default"
        self.move_to(tx, ty, cursor_mode=arrive_mode)
        return tx, ty

    def smart_click(
        self,
        target: str | Locator,
        *,
        label: str = "",
        timeout_ms: int = 8_000,
        hover_ms: tuple[int, int] = (120, 480),
    ) -> bool:
        """Rà chuột cong → hover ngẫu nhiên → click."""
        loc = target if isinstance(target, Locator) else self.page.locator(str(target)).first
        try:
            loc.wait_for(state="visible", timeout=timeout_ms)
            loc.scroll_into_view_if_needed(timeout=timeout_ms)
        except Exception as exc:  # noqa: BLE001
            logger.debug("[HumanAction] smart_click chờ visible {}: {}", label, exc)
            return False
        try:
            self.move_to_locator(loc)
            pause = random_ms(hover_ms[0], hover_ms[1]) / 1000.0
            x, y = self._pos()
            self._update_virtual_cursor(x, y, mode="pointer")
            time.sleep(pause)
            self.page.mouse.click(x, y, delay=random.randint(45, 160))
            self._update_virtual_cursor(x, y, mode="pointer")
            tag = label or (str(target)[:40] if isinstance(target, str) else "locator")
            logger.debug("[HumanAction] smart_click OK — {}", tag)
            return True
        except Exception as exc:  # noqa: BLE001
            logger.warning("[HumanAction] smart_click thất bại {}: {}", label, exc)
            return False

    def scroll_to_top(self) -> None:
        """Đưa viewport về đầu trang trước khi cuộn xuống dần (giống mở feed từ trên)."""
        try:
            self.page.evaluate("() => window.scrollTo({ top: 0, left: 0, behavior: 'instant' })")
        except Exception:
            try:
                self.page.evaluate("() => window.scrollTo(0, 0)")
            except Exception:
                pass
        time.sleep(random.uniform(0.8, 1.8))

    def natural_scroll_feed(
        self,
        *,
        rounds: int | None = None,
        like_rate: float = 0.30,
        comment_rate: float = 0.0,
        on_like: Any | None = None,
        on_comment: Any | None = None,
        downward_bias: float = 0.97,
        scroll_from_top: bool = False,
        dwell_scale: float = 1.0,
    ) -> None:
        """
        Cuộn bảng tin kiểu đọc: ưu tiên **từ trên xuống dưới**, dừng lâu giữa các lần cuộn.

        ``downward_bias`` gần 1.0 → hầu hết chỉ cuộn xuống; ``scroll_from_top`` → scrollTo(0) trước.
        """
        lo = _env_int("FB_FEED_SCROLL_MIN", 14)
        hi = max(lo, _env_int("FB_FEED_SCROLL_MAX", 24))
        n = rounds if rounds is not None else random.randint(lo, hi)
        bias = max(0.5, min(1.0, float(downward_bias)))
        scale = max(0.75, float(dwell_scale))
        if scroll_from_top:
            self.scroll_to_top()
            time.sleep(random.uniform(1.0, 2.2))
        logger.info(
            "[HumanAction] Cuộn {} vòng | top→down bias={:.0%} | dwell×{:.2f}",
            n,
            bias,
            scale,
        )
        for i in range(n):
            if random.random() > bias:
                dy = -random.randint(50, 140)
            else:
                dy = random.randint(440, 920)
            self._scroll_by(dy)
            has_media = random.random() < 0.52
            base_lo = 2800 if has_media else 2200
            base_hi = 9000 if has_media else 5500
            dwell_ms = int(random.randint(base_lo, base_hi) * scale)
            if random.random() < 0.22:
                dwell_ms += int(random.randint(1800, 4500) * scale)
            if random.random() < 0.22:
                time.sleep(random.uniform(0.7, 1.8) * scale)
            self.page.wait_for_timeout(dwell_ms)
            if on_like and random.random() < max(0.0, min(1.0, like_rate)):
                try:
                    on_like(self.page)
                except Exception as exc:  # noqa: BLE001
                    logger.debug("[HumanAction] on_like: {}", exc)
            if on_comment and random.random() < max(0.0, min(1.0, comment_rate)):
                try:
                    on_comment(self.page)
                except Exception as exc:  # noqa: BLE001
                    logger.debug("[HumanAction] on_comment: {}", exc)
            if i < n - 1 and random.random() < max(0.04, 0.22 * (1.0 - bias)):
                self._scroll_by(-random.randint(40, 120))
                self.page.wait_for_timeout(random.randint(1800, 4500))

    def _scroll_by(self, dy: int) -> None:
        """Cuộn từng bước nhỏ (wheel) thay vì nhảy một lần."""
        steps = max(1, min(12, abs(dy) // 70))
        step = dy / steps
        for _ in range(steps):
            try:
                self.page.mouse.wheel(0, step)
            except Exception:
                self.page.evaluate("(y) => window.scrollBy(0, y)", step)
            time.sleep(random.uniform(0.04, 0.16))

    def smart_type_search(
        self,
        locator: Locator,
        text: str,
        *,
        label: str = "search",
        already_focused: bool = False,
    ) -> None:
        """Gõ tìm kiếm chậm, verify đủ chữ, rồi mới Enter."""
        from src.utils.human_typing import human_type_search_locator

        human_type_search_locator(
            locator,
            text,
            delay_min_ms=_env_int("FB_SEARCH_TYPING_MIN_MS", 110),
            delay_max_ms=_env_int("FB_SEARCH_TYPING_MAX_MS", 420),
            pause_before_enter_ms=(2400, 4500),
            label=label,
            already_focused=already_focused,
        )


def smart_click(page: Page, target: str | Locator, **kwargs: Any) -> bool:
    """Shortcut module-level."""
    return HumanAction(page).smart_click(target, **kwargs)
