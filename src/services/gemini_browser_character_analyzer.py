from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path
from typing import Any

from loguru import logger
from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth


def _extract_balanced_json_arrays(s: str) -> list[str]:
    out: list[str] = []
    start = -1
    depth = 0
    in_str = False
    esc = False
    for i, ch in enumerate(s):
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
            continue
        if ch == "[":
            if depth == 0:
                start = i
            depth += 1
        elif ch == "]" and depth > 0:
            depth -= 1
            if depth == 0 and start >= 0:
                out.append(s[start : i + 1])
                start = -1
    return out


def _looks_like_character_row(row: Any) -> bool:
    if not isinstance(row, dict):
        return False
    name = str(row.get("name", "")).strip()
    role = str(row.get("role", "")).strip()
    appearance = str(row.get("appearance", "")).strip()
    if name in {"...", "…"}:
        return False
    # Loại các object schema placeholder.
    if any(str(v).strip() in {"...", "…"} for v in row.values()):
        return False
    return bool(name) and bool(role or appearance)


def _score_character_array(rows: list[Any]) -> int:
    if not rows:
        return -1
    valid_rows = [r for r in rows if _looks_like_character_row(r)]
    if not valid_rows:
        return -1
    # Ưu tiên danh sách có nhiều nhân vật hợp lệ hơn.
    return len(valid_rows) * 100 + len(rows)


def _extract_json_array(raw: str) -> list[dict[str, Any]]:
    s = str(raw or "").strip()
    if not s:
        return []
    candidates: list[str] = []
    for m in re.finditer(r"```(?:json)?\s*([\s\S]*?)\s*```", s, flags=re.IGNORECASE):
        chunk = str(m.group(1) or "").strip()
        if chunk:
            candidates.append(chunk)
    candidates.append(s)

    best_rows: list[dict[str, Any]] = []
    best_score = -1
    for text in candidates:
        for blob in _extract_balanced_json_arrays(text):
            try:
                data = json.loads(blob)
            except Exception:
                continue
            if not isinstance(data, list):
                continue
            score = _score_character_array(data)
            if score > best_score:
                best_score = score
                best_rows = [x for x in data if isinstance(x, dict)]
    return best_rows


def _build_prompt(*, script: str, topic: str, language_display: str, style_hint: str, max_characters: int = 12) -> str:
    lang = str(language_display or "Tiếng Việt").strip()
    combined = f"{str(script or '').strip()}\n\n{str(topic or '').strip()}".strip()
    if len(combined) > 14000:
        combined = combined[:14000]
    style_block = str(style_hint or "").strip()[:600]
    return f"""Bạn là chuyên gia phân tích kịch bản để tạo danh sách nhân vật cho video AI.

Nhiệm vụ: đọc kịch bản và liệt kê cast cần giữ đồng nhất hình ảnh.
Ngôn ngữ mô tả: {lang}
Số lượng tối đa: {max_characters}
Gợi ý style: {style_block or "(không có)"}

Chỉ trả về JSON array hợp lệ, không markdown, không giải thích.
Chỉ giữ đúng các field nhân vật sau (KHÔNG thêm key khác):
name, role, gender, age, appearance, facial_features, outfit, personality, consistency_note
Schema:
[
  {{
    "name":"...",
    "role":"main_character|support|child|pet|other",
    "gender":"male|female|unspecified",
    "age":"...",
    "appearance":"...",
    "facial_features":"...",
    "outfit":"...",
    "personality":"...",
    "consistency_note":"..."
  }}
]

Kịch bản:
{combined}
""".strip()


def _resolve_profile_dir(preferred_profile_dir: str = "") -> Path:
    p = str(preferred_profile_dir or "").strip()
    if not p:
        p = os.environ.get("NANOBANANA_BROWSER_PROFILE_DIR", "").strip() or os.environ.get("VEO3_BROWSER_PROFILE_DIR", "").strip()
    d = Path(p) if p else (Path("data") / "nanobanana" / "browser_profile")
    d.mkdir(parents=True, exist_ok=True)
    return d.resolve()


def infer_characters_via_gemini_browser(
    *,
    script: str,
    topic: str = "",
    language_display: str = "Tiếng Việt",
    style_hint: str = "",
    preferred_profile_dir: str = "",
    headless: bool | None = None,
) -> tuple[list[dict[str, Any]], str]:
    """
    Chạy phân tích nhân vật bằng Gemini browser profile đã đăng nhập.
    Browser chạy headless mặc định (ẩn), có thể bật bằng GEMINI_BROWSER_HEADLESS=0.
    """
    prompt = _build_prompt(script=script, topic=topic, language_display=language_display, style_hint=style_hint)
    profile_dir = _resolve_profile_dir(preferred_profile_dir)
    if headless is None:
        headless = str(os.environ.get("GEMINI_BROWSER_HEADLESS", "1")).strip().lower() not in {"0", "false", "no", "off"}
    url = "https://gemini.google.com/app"
    try:
        with sync_playwright() as pw:
            context = pw.chromium.launch_persistent_context(
                user_data_dir=str(profile_dir),
                headless=headless,
                locale="en-US",
                viewport={"width": 1366, "height": 900},
                channel="chrome",
                args=["--lang=en-US"],
            )
            try:
                stealth = Stealth()
                page = context.new_page() if not context.pages else context.pages[0]
                try:
                    stealth.apply_stealth_sync(page)
                except Exception:
                    pass
                page.goto(url, wait_until="domcontentloaded", timeout=60_000)
                box = page.locator(
                    "textarea, div[contenteditable='true'][aria-label*='Message'], div[contenteditable='true'][data-placeholder]"
                ).first
                box.wait_for(timeout=25_000)
                try:
                    box.click(timeout=8_000)
                except Exception:
                    pass
                try:
                    box.fill(prompt)
                except Exception:
                    page.keyboard.type(prompt, delay=8)
                page.keyboard.press("Enter")

                deadline = time.time() + 90
                last_text = ""
                while time.time() < deadline:
                    time.sleep(2.0)
                    try:
                        txt = page.locator("main").inner_text(timeout=3_000)
                    except Exception:
                        txt = page.content()
                    if txt and txt != last_text:
                        last_text = txt
                        rows = _extract_json_array(txt)
                        if rows:
                            return _normalize_character_rows(rows), ""
                return [], "Không đọc được JSON cast từ Gemini browser (timeout)."
            finally:
                context.close()
    except Exception as exc:  # noqa: BLE001
        logger.warning("Gemini browser character analyze lỗi: {}", exc)
        return [], f"Lỗi Gemini browser: {exc}"


def _normalize_character_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in list(rows or []):
        if not isinstance(row, dict):
            continue
        name = str(row.get("name", "")).strip()
        if not name:
            continue
        out.append(
            {
                "name": name,
                "role": str(row.get("role", "support")).strip() or "support",
                "gender": str(row.get("gender", "unspecified")).strip() or "unspecified",
                "age": str(row.get("age", "")).strip() or "—",
                "appearance": str(row.get("appearance", "")).strip() or "consistent appearance",
                "facial_features": str(row.get("facial_features", "")).strip() or "stable facial identity",
                "outfit": str(row.get("outfit", "")).strip() or "consistent outfit",
                "personality": str(row.get("personality", "")).strip() or "natural personality",
                "consistency_note": str(row.get("consistency_note", "")).strip() or "keep identity stable across episodes",
                "reference_image_path": str(row.get("reference_image_path", "")).strip(),
            }
        )
    return out

