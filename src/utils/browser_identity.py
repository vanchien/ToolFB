"""Gắn số thứ tự và UID lên chính cửa sổ Firefox (tab + góc trang)."""

from __future__ import annotations

import json
from typing import Any

from loguru import logger

_PAINT_JS = """
(label) => {
  const paint = () => {
    const root = document.documentElement;
    if (!root) return;
    let el = document.getElementById('toolfb-stt-badge');
    if (!el) {
      el = document.createElement('div');
      el.id = 'toolfb-stt-badge';
      el.setAttribute('aria-hidden', 'true');
      root.appendChild(el);
    }
    el.textContent = label;
    el.style.cssText = 'position:fixed;top:8px;left:8px;z-index:2147483647;pointer-events:none;max-width:92vw;padding:7px 14px;border-radius:8px;background:#0f172a;color:#f8fafc;font:700 18px/1.2 Segoe UI,sans-serif;box-shadow:0 2px 12px rgba(0,0,0,.5);letter-spacing:.2px';
    const cur = String(document.title || 'Facebook');
    if (!cur.startsWith(label)) {
      const rest = cur.replace(/^#\\d+\\s·\\s.+?\\s—\\s/, '') || 'Facebook';
      document.title = label + ' — ' + rest;
    }
  };
  paint();
  if (window.__toolfbSttTimer && window.__toolfbSttLabel === label) return;
  window.__toolfbSttLabel = label;
  if (window.__toolfbSttTimer) {
    try { clearInterval(window.__toolfbSttTimer); } catch (e) {}
  }
  window.__toolfbSttTimer = setInterval(() => { try { paint(); } catch (e) {} }, 1000);
}
"""


def format_account_window_label(queue_no: int, uid: str) -> str:
    """Nhãn hiển thị trong trình duyệt, ví dụ ``#03 · 61582438296851``."""
    text = str(uid or "").strip()
    if text.upper().startswith("UID_"):
        text = text[4:]
    n = int(queue_no or 0)
    if n > 0 and text:
        return f"#{n:02d} · {text}"
    if n > 0:
        return f"#{n:02d}"
    return text


def _init_source(label: str) -> str:
    """Script chạy lại sau mỗi lần Facebook tải trang — IIFE, không phải hàm trần."""
    payload = json.dumps(label, ensure_ascii=False)
    return f"(() => {{ ({_PAINT_JS.strip()})({payload}); }})();"


def stamp_browser_account_label(
    context: Any,
    page: Any,
    label: str,
    profile_path: str = "",
    *,
    install_init: bool = True,
    title_timeout_s: float = 2.0,
) -> None:
    """
    Hiện ``#STT · UID`` trên tab Firefox và một nhãn góc trái trang.

    Facebook ghi đè ``document.title`` sau khi tải, nên script tự gắn lại mỗi giây.
    """
    text = str(label or "").strip()
    if not text:
        return
    if install_init and context is not None:
        try:
            context.add_init_script(_init_source(text))
        except Exception as exc:  # noqa: BLE001
            logger.debug("[Human] Không gắn script nhãn tài khoản: {}", exc)
    _paint_now(page, text)
    prof = str(profile_path or "").strip()
    if not prof:
        return
    try:
        from src.utils.win_browser_window import set_firefox_window_title

        set_firefox_window_title(prof, text, timeout_s=title_timeout_s)
    except Exception as exc:  # noqa: BLE001
        logger.debug("[Human] Chưa ghi được tiêu đề cửa sổ: {}", exc)


def _paint_now(page: Any, label: str) -> None:
    try:
        if page is None or page.is_closed():
            return
        page.evaluate(_PAINT_JS, label)
    except Exception:
        pass
