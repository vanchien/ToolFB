"""
Gõ phím giống người dùng — không gán giá trị bằng ``fill()``.

Mỗi ký tự: ``locator.press`` + tạm dừng ngẫu nhiên (mặc định 50–200 ms).
Có thể bấm Enter sau khi gõ xong (form đăng nhập / TOTP).
"""

from __future__ import annotations

import os
import random
import time
import unicodedata
from typing import Any

from loguru import logger

from src.utils.delays import random_ms


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(float(raw))
    except (TypeError, ValueError):
        return default


def human_typing_delay_ms() -> int:
    """Một lần delay giữa hai ký tự (ms), ngẫu nhiên trong khoảng cấu hình."""
    lo = max(1, _env_int("FB_TYPING_DELAY_MIN_MS", 50))
    hi = max(lo, _env_int("FB_TYPING_DELAY_MAX_MS", 200))
    return random_ms(lo, hi)


def human_type_locator(
    locator: Any,
    text: str,
    *,
    submit_enter: bool = False,
    clear_first: bool = True,
    label: str = "",
) -> None:
    """
    Gõ ``text`` vào ô đã focus — từng ký tự, không dùng ``fill()``.

    Args:
        locator: Playwright ``Locator`` (ô input / textbox).
        text: Chuỗi cần gõ.
        submit_enter: True → ``Enter`` sau khi gõ xong.
        clear_first: True → Ctrl+A, Backspace trước khi gõ.
        label: Ghi log (tùy chọn).
    """
    payload = str(text or "")
    tag = f" [{label}]" if label else ""
    locator.click(timeout=5_000)
    if clear_first and payload:
        try:
            locator.press("Control+A", timeout=1_500)
            locator.press("Backspace", timeout=1_500)
        except Exception:
            try:
                locator.press("Home", timeout=800)
                for _ in range(min(len(payload) + 8, 64)):
                    locator.press("Backspace", timeout=300)
            except Exception:
                pass
    lo = max(1, _env_int("FB_TYPING_DELAY_MIN_MS", 50))
    hi = max(lo, _env_int("FB_TYPING_DELAY_MAX_MS", 200))
    for ch in payload:
        locator.press(ch, timeout=4_000)
        time.sleep(random_ms(lo, hi) / 1000.0)
    if submit_enter:
        locator.press("Enter", timeout=5_000)
    logger.debug(
        "human_type{} | len={} | delay={}–{}ms | enter={}",
        tag,
        len(payload),
        lo,
        hi,
        submit_enter,
    )


def _normalize_typed(s: str) -> str:
    return unicodedata.normalize("NFC", str(s or "").strip().lower())


def _read_locator_text(locator: Any) -> str:
    """Đọc nội dung ô tìm kiếm (input, combobox, contenteditable con)."""
    try:
        return str(
            locator.evaluate(
                """el => {
                  if (!el) return '';
                  const active = document.activeElement;
                  const pick = (n) => {
                    if (!n) return '';
                    if (n.isContentEditable) return (n.innerText || n.textContent || '').trim();
                    if ('value' in n && n.value) return String(n.value).trim();
                    return (n.textContent || n.innerText || '').trim();
                  };
                  let t = pick(el);
                  if (!t && active && (el.contains(active) || active.contains(el))) t = pick(active);
                  if (!t) {
                    const inner = el.querySelector(
                      'input,[contenteditable="true"],[role="combobox"]'
                    );
                    if (inner) t = pick(inner);
                  }
                  return t;
                }"""
            )
            or ""
        ).strip()
    except Exception:
        return ""


def _wait_search_query_ready(
    locator: Any,
    payload: str,
    *,
    delay_lo_ms: int,
    delay_hi_ms: int,
    max_sec: float = 14.0,
) -> bool:
    """
    Chờ ô search chứa **đủ** từ khóa (đọc ổn định 2 lần liên tiếp).

    Nếu thiếu ký tự → gõ bổ sung trước khi cho phép Enter.
    """
    target = _normalize_typed(payload)
    if not target:
        return True
    deadline = time.monotonic() + max(2.0, float(max_sec))
    stable_hits = 0
    last_current = ""
    while time.monotonic() < deadline:
        current = _normalize_typed(_read_locator_text(locator))
        ok = current == target or (len(target) >= 4 and target in current)
        if ok:
            if current == last_current:
                stable_hits += 1
                if stable_hits >= 2:
                    return True
            else:
                stable_hits = 1
            last_current = current
        else:
            stable_hits = 0
            last_current = current
            if current and target.startswith(current):
                missing = payload[len(current) :]
            else:
                missing = payload
            if missing:
                for ch in missing:
                    locator.press(ch, timeout=4_000)
                    time.sleep(random_ms(delay_lo_ms, delay_hi_ms) / 1000.0)
            elif not current:
                for ch in payload:
                    locator.press(ch, timeout=4_000)
                    time.sleep(random_ms(delay_lo_ms, delay_hi_ms) / 1000.0)
        time.sleep(random.uniform(0.2, 0.42))
    final = _normalize_typed(_read_locator_text(locator))
    return final == target or (len(target) >= 4 and target in final)


def human_type_search_locator(
    locator: Any,
    text: str,
    *,
    delay_min_ms: int = 80,
    delay_max_ms: int = 350,
    pause_before_enter_ms: tuple[int, int] = (1200, 2800),
    label: str = "search",
    already_focused: bool = False,
) -> None:
    """
    Gõ từ khóa tìm kiếm — **chỉ Enter sau khi gõ đủ**, có verify nội dung ô.

    Tránh Facebook gửi tìm kiếm khi mới gõ một phần (typeahead / combobox).
    """
    payload = str(text or "")
    if not payload:
        return
    lo = max(40, int(delay_min_ms))
    hi = max(lo, int(delay_max_ms))
    tag = f" [{label}]" if label else ""
    if not already_focused:
        locator.click(timeout=5_000)
        time.sleep(random_ms(400, 900) / 1000.0)
    try:
        locator.press("Control+A", timeout=1_500)
        locator.press("Backspace", timeout=1_500)
    except Exception:
        pass
    time.sleep(random_ms(200, 500) / 1000.0)

    for i, ch in enumerate(payload):
        locator.press(ch, timeout=5_000)
        time.sleep(random_ms(lo, hi) / 1000.0)
        if random.random() < 0.12:
            time.sleep(random.uniform(0.3, 0.75))
        if (i + 1) % 3 == 0:
            time.sleep(random.uniform(0.2, 0.55))

    ready = _wait_search_query_ready(
        locator,
        payload,
        delay_lo_ms=lo,
        delay_hi_ms=hi,
        max_sec=16.0,
    )
    if not ready:
        logger.warning(
            "human_type_search{} | ô chưa khớp đủ từ khóa trước Enter — vẫn thử Enter (đọc={!r})",
            tag,
            _read_locator_text(locator)[:80],
        )

    dwell = random_ms(pause_before_enter_ms[0], pause_before_enter_ms[1]) / 1000.0
    time.sleep(dwell)
    locator.press("Enter", timeout=6_000)
    time.sleep(random.uniform(0.9, 1.6))
    logger.info(
        "human_type_search{} | len={} | delay={}–{}ms | dwell_trước_enter={:.1f}s",
        tag,
        len(payload),
        lo,
        hi,
        dwell,
    )
