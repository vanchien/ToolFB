"""
Click / cuộn trong trang qua DOM — không đụng chuột hay bàn phím hệ thống.

``page.mouse.*`` và ``page.keyboard.press`` trên Windows (Firefox headed) dễ chiếm
con trỏ/focus của người dùng. Dùng ``el.click()`` / ``scrollBy`` trong page.
"""

from __future__ import annotations

from typing import Any

from loguru import logger

_CLICK_NODE_JS = """(el) => {
  if (!el) return false;
  try {
    if (typeof el.scrollIntoView === 'function') {
      el.scrollIntoView({ block: 'center', inline: 'nearest' });
    }
  } catch (_e) {}
  try {
    if (typeof el.focus === 'function') el.focus({ preventScroll: true });
  } catch (_e2) {}
  try {
    if (typeof el.click === 'function') {
      el.click();
      return true;
    }
  } catch (_e3) {}
  try {
    el.dispatchEvent(new MouseEvent('click', {
      bubbles: true, cancelable: true, view: window, buttons: 1
    }));
    return true;
  } catch (_e4) {
    return false;
  }
}"""

_CLICK_POINT_JS = """([x, y]) => {
  const el = document.elementFromPoint(Number(x), Number(y));
  if (!el) return false;
  try {
    if (typeof el.click === 'function') {
      el.click();
      return true;
    }
  } catch (_e) {}
  try {
    el.dispatchEvent(new MouseEvent('click', {
      bubbles: true, cancelable: true, clientX: Number(x), clientY: Number(y), view: window, buttons: 1
    }));
    return true;
  } catch (_e2) {
    return false;
  }
}"""

_SCROLL_BY_JS = """([dx, dy]) => {
  const e = document.scrollingElement || document.documentElement || document.body;
  if (!e) return false;
  e.scrollBy(Number(dx) || 0, Number(dy) || 0);
  return true;
}"""

CHROMIUM_BACKGROUND_ARGS = (
    "--mute-audio",
    "--disable-notifications",
    "--no-first-run",
    "--no-default-browser-check",
)


def click_locator_dom(locator: Any, *, timeout_ms: int = 3_000) -> bool:
    """Bấm locator bằng ``el.click()`` trong trang. Không dùng chuột OS."""
    if locator is None:
        return False
    try:
        loc = locator.first if hasattr(locator, "first") else locator
        try:
            loc.wait_for(state="attached", timeout=max(400, int(timeout_ms)))
        except Exception:
            pass
        return bool(loc.evaluate(_CLICK_NODE_JS))
    except Exception as exc:  # noqa: BLE001
        logger.debug("click_locator_dom: {}", exc)
        return False


def click_point_dom(page: Any, x: float, y: float) -> bool:
    """Bấm tọa độ viewport bằng ``elementFromPoint`` — không ``page.mouse``."""
    if page is None:
        return False
    try:
        return bool(page.evaluate(_CLICK_POINT_JS, [float(x), float(y)]))
    except Exception as exc:  # noqa: BLE001
        logger.debug("click_point_dom: {}", exc)
        return False


def scroll_by_dom(page: Any, dy: float, dx: float = 0.0) -> bool:
    """Cuộn trang bằng ``scrollBy``. Không ``page.mouse.wheel`` / ``keyboard.press``."""
    if page is None:
        return False
    try:
        return bool(page.evaluate(_SCROLL_BY_JS, [float(dx), float(dy)]))
    except Exception as exc:  # noqa: BLE001
        logger.debug("scroll_by_dom: {}", exc)
        return False


def chromium_background_launch_kwargs(*, headless: bool) -> dict[str, Any]:
    """Tham số launch Chromium cho quét/tải nền — không bật tiếng, không thông báo."""
    return {
        "headless": bool(headless),
        "args": list(CHROMIUM_BACKGROUND_ARGS),
    }
