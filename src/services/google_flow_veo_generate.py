"""
Bước chuẩn: nhập prompt xong → bấm nút tạo video → chờ Veo / Flow bắt đầu generate.

Áp dụng cho Google Labs Flow (Veo 3), ví dụ: https://labs.google/fx/vi/tools/flow
"""

from __future__ import annotations

import json
import os
import random
import re
import time
from datetime import datetime
from pathlib import Path

from loguru import logger
from playwright.sync_api import Locator, Page, TimeoutError as PlaywrightTimeoutError, sync_playwright

from src.utils.paths import project_root

GOOGLE_FLOW_URL = "https://labs.google/fx/vi/tools/flow"

_FLOW_BUTTON_NAMES = re.compile(
    r"Generate|Create|Start|Submit|Send|Tạo\s*video|Tạo|Bắt đầu|Gửi",
    re.I,
)

_GENERATION_TEXT = re.compile(
    r"Generating|Đang\s*tạo|Creating|Rendering|Processing",
    re.I,
)

_FLOW_NEW_PROJECT_NAMES = re.compile(
    r"New project|Create project|Start new project|Tạo dự án mới|Dự án mới|Tạo mới|Bắt đầu|Start",
    re.I,
)

_FLOW_MODE_NAMES = re.compile(
    r"Text to video|Từ văn bản sang video|From text to video|Video|Veo|Veo 3",
    re.I,
)


def _save_failure_screenshot(page: Page, *, reason: str) -> Path | None:
    """Lưu screenshot khi thao tác generate thất bại (theo quy ước logs/screenshots)."""
    try:
        d = project_root() / "logs" / "screenshots"
        d.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe = re.sub(r"[^a-zA-Z0-9_-]+", "_", reason).strip("_")[:80] or "flow_generate_err"
        p = d / f"google_flow_generate_{safe}_{ts}.png"
        page.screenshot(path=str(p), full_page=True)
        logger.warning("Đã chụp screenshot lỗi Google Flow generate: {}", p)
        return p
    except Exception:
        return None


def dismiss_obvious_overlays(page: Page) -> None:
    """Thử đóng overlay phổ biến (Escape) để nút không bị che."""
    for _ in range(2):
        try:
            page.keyboard.press("Escape")
            time.sleep(0.2)
        except Exception:
            break


def has_prompt_box(page: Page) -> bool:
    """True nếu đã thấy ô nhập prompt (đang trong workspace / project)."""
    selectors = [
        "textarea",
        "[contenteditable='true']",
        "[role='textbox']",
        "div[contenteditable='true']",
    ]
    for selector in selectors:
        try:
            loc = page.locator(selector).last
            if loc.count() > 0 and loc.is_visible(timeout=1000):
                return True
        except Exception:
            continue
    return False


def check_google_flow_login(page: Page) -> bool:
    """
    True nếu coi như đã đăng nhập Flow.
    Nếu body có chữ đăng nhập/sign in/login và chưa có prompt box → chưa login.
    """
    try:
        body = page.locator("body").inner_text(timeout=10_000).lower()
    except Exception:
        return True
    needles = ("đăng nhập", "sign in", "login")
    if not any(x in body for x in needles):
        return True
    if has_prompt_box(page):
        return True
    return False


def open_or_create_flow_project(page: Page) -> None:
    """
    Tự tạo / mở project Flow. Nếu đã có prompt box thì bỏ qua.
    """
    if has_prompt_box(page):
        return

    # 0) Selector đặc thù theo HTML thực tế:
    # <button> <i>add_2</i> Dự án mới ... </button>
    specific_selectors = [
        "button:has(i:has-text('add_2')):has-text('Dự án mới')",
        "button:has-text('Dự án mới')",
        "button:has-text('New project')",
    ]
    for sel in specific_selectors:
        try:
            btn = page.locator(sel).last
            if btn.count() == 0:
                continue
            btn.wait_for(state="visible", timeout=5000)
            try:
                btn.scroll_into_view_if_needed(timeout=1500)
            except Exception:
                pass
            try:
                btn.click(timeout=5000)
            except Exception:
                # overlay có thể chặn pointer events, dùng force click
                btn.click(timeout=5000, force=True)
            page.wait_for_timeout(3000)
            if has_prompt_box(page):
                return
        except Exception:
            continue

    # 1) Ưu tiên role chuẩn trước.
    for role in ("button", "link"):
        try:
            btn = page.get_by_role(role, name=_FLOW_NEW_PROJECT_NAMES).first
            btn.wait_for(state="visible", timeout=8000)
            btn.click(timeout=5000)
            page.wait_for_timeout(3000)
            if has_prompt_box(page):
                return
        except PlaywrightTimeoutError:
            pass
        except Exception:
            pass

    # 2) Fallback theo text (bao gồm dạng chip nổi '+ Dự án mới').
    for sel in (
        "text=/\\+\\s*Dự án mới/i",
        "text=/Dự án mới/i",
        "text=/New project/i",
        "text=/Create project/i",
    ):
        try:
            loc = page.locator(sel).last
            if loc.count() == 0:
                continue
            loc.wait_for(state="visible", timeout=6000)
            try:
                loc.scroll_into_view_if_needed(timeout=1500)
            except Exception:
                pass
            try:
                loc.click(timeout=5000)
            except Exception:
                # Một số build Flow render text trong div non-clickable -> click vị trí trung tâm.
                box = loc.bounding_box()
                if box:
                    page.mouse.click(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)
                else:
                    raise
            page.wait_for_timeout(3200)
            if has_prompt_box(page):
                return
        except Exception:
            continue

    # 3) Fallback JS: tìm node chứa text "Dự án mới/New project" và click element clickable gần nhất.
    try:
        js_clicked = bool(
            page.evaluate(
                """() => {
                    const re = /\\+?\\s*(Dự án mới|New project|Create project)/i;
                    const nodes = Array.from(document.querySelectorAll('button, a, div, span, i'));
                    for (let i = nodes.length - 1; i >= 0; i--) {
                        const el = nodes[i];
                        const txt = (el.innerText || el.textContent || '').trim();
                        const icon = (el.querySelector && el.querySelector('i')) ? ((el.querySelector('i').innerText || '').trim()) : '';
                        if ((!txt || !re.test(txt)) && icon !== 'add_2') continue;
                        let target = el;
                        const clickable = el.closest('button, a, [role="button"], [tabindex]');
                        if (clickable) target = clickable;
                        const st = window.getComputedStyle(target);
                        const rect = target.getBoundingClientRect();
                        if (!rect.width || !rect.height) continue;
                        if (st.visibility === 'hidden' || st.display === 'none') continue;
                        target.click();
                        return true;
                    }
                    return false;
                }"""
            )
        )
        if js_clicked:
            page.wait_for_timeout(3200)
            if has_prompt_box(page):
                return
    except Exception:
        pass

    try:
        text_btn = page.get_by_text(_FLOW_NEW_PROJECT_NAMES).first
        text_btn.wait_for(state="visible", timeout=5000)
        text_btn.click(timeout=5000)
        page.wait_for_timeout(3000)
        if has_prompt_box(page):
            return
    except PlaywrightTimeoutError:
        pass
    except Exception:
        pass

    if has_prompt_box(page):
        return
    raise RuntimeError("Không tìm thấy nút tạo project mới trong Google Flow.")


def _discover_flow_model_choices(page: Page) -> list[str]:
    """Đọc model options đang hiển thị trong menu Flow."""
    script = """
(() => {
  const out = [];
  const re = /(Veo\\s*3(\\.1)?\\s*[-–]?\\s*(Fast|Lite|Quality)?(?:\\s*\\[Lower Priority\\])?(?:\\s*\\(.*?\\))?|Nano\\s*Banana\\s*2)/i;
  const menus = Array.from(document.querySelectorAll('[role="menu"][data-state="open"], [data-radix-dropdown-menu-content][data-state="open"]'));
  const menuItems = menus.length > 0
    ? menus.flatMap((m) => Array.from(m.querySelectorAll('[role="menuitem"], [role="option"]')))
    : Array.from(document.querySelectorAll('[role="menuitem"], [role="option"]'));
  for (const item of menuItems) {
    const spans = Array.from(item.querySelectorAll('span'));
    let txt = '';
    for (const sp of spans) {
      const t = (sp.textContent || '').replace(/\\s+/g, ' ').trim();
      if (t.length > txt.length) txt = t;
    }
    if (!txt) {
      txt = (item.textContent || '').replace(/\\s+/g, ' ').trim();
    }
    if (!txt || !re.test(txt)) continue;
    out.push(txt);
  }
  return Array.from(new Set(out));
})()
"""
    try:
        got = page.evaluate(script)
    except Exception:
        return []
    if not isinstance(got, list):
        return []
    out: list[str] = []
    for x in got:
        s = str(x).strip()
        if s and s not in out:
            out.append(s)
    return out


def _flow_model_cache_path() -> Path:
    p = project_root() / "data" / "google_flow_video" / "temp" / "flow_model_choices.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def _save_flow_model_cache(models: list[str]) -> None:
    uniq: list[str] = []
    for x in models:
        s = str(x).strip()
        if s and s not in uniq:
            uniq.append(s)
    payload = {
        "models": uniq,
        "updated_at": datetime.now().replace(microsecond=0).isoformat(),
    }
    _flow_model_cache_path().write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def sync_flow_model_choices_from_profile(*, profile_dir: str, web_url: str = "") -> list[str]:
    """
    Mở profile Flow hiện có, đọc danh sách model từ dropdown và lưu cache.
    Trả về danh sách model UI đang hiển thị (có thể gồm Lower Priority).
    """
    target_url = str(web_url or "").strip() or GOOGLE_FLOW_URL
    pdir = str(profile_dir or "").strip()
    if not pdir:
        raise ValueError("Thiếu profile_dir để đồng bộ model Flow.")

    with sync_playwright() as pw:
        launch_kwargs: dict[str, object] = {}
        ch = str(os.environ.get("GOOGLE_FLOW_CHROMIUM_CHANNEL", "chrome")).strip().lower()
        if ch and ch not in {"0", "false", "off", "bundled", "playwright", "chromium"}:
            launch_kwargs["channel"] = ch
        ctx = pw.chromium.launch_persistent_context(
            user_data_dir=pdir,
            headless=False,
            viewport={"width": 1366, "height": 900},
            locale="en-US",
            accept_downloads=False,
            **launch_kwargs,
        )
        try:
            page = ctx.pages[0] if ctx.pages else ctx.new_page()
            page.goto(target_url, wait_until="domcontentloaded", timeout=60_000)
            page.wait_for_timeout(900)
            open_or_create_flow_project(page)
            select_text_to_video_mode_if_needed(page)
            models = _discover_flow_model_choices(page)
            if models:
                _save_flow_model_cache(models)
            return models
        finally:
            try:
                ctx.close()
            except Exception:
                pass


def _best_model_match(requested: str, choices: list[str]) -> str:
    req = str(requested or "").strip()
    if not req:
        return ""
    if not choices:
        return req
    req_l = req.lower()

    def _norm(x: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", str(x or "").lower())

    for c in choices:
        if c.lower() == req_l:
            return c
    req_n = _norm(req)
    for c in choices:
        c_n = _norm(c)
        if req_n and (req_n in c_n or c_n in req_n):
            return c
    if "fast" in req_l:
        for c in choices:
            if "fast" in c.lower():
                return c
    if "lite" in req_l:
        for c in choices:
            if "lite" in c.lower():
                return c
    if "quality" in req_l or "generate-preview" in req_l:
        for c in choices:
            cl = c.lower()
            if "quality" in cl or ("veo" in cl and "fast" not in cl and "lite" not in cl):
                return c
    if "veo" in req_l:
        for c in choices:
            if "veo" in c.lower():
                return c
    return choices[0]


def _try_select_veo3_model(page: Page, requested_model: str = "") -> None:
    """
    Chọn model Veo theo model user yêu cầu (nếu có),
    fallback về Veo mặc định; không fail luồng nếu không thấy.
    """
    def _safe_click(loc) -> bool:
        try:
            loc.click(timeout=2200)
            return True
        except Exception:
            pass
        try:
            loc.click(timeout=2200, force=True)
            return True
        except Exception:
            pass
        try:
            handle = loc.element_handle(timeout=1200)
            if handle is not None:
                page.evaluate("(el) => el.click()", handle)
                return True
        except Exception:
            pass
        return False

    # Bước 1: mở menu model (nếu đang là chip/badge).
    opened = False
    open_selectors = (
        "button[aria-haspopup='menu'][data-state='closed']:has-text('Veo 3.1')",
        "button[aria-haspopup='menu'][data-state='closed']:has-text('Veo')",
        "button[aria-haspopup='menu']:has-text('Veo 3.1 - Fast')",
        "button[aria-haspopup='menu']:has-text('Veo 3.1 - Lite [Lower Priority]')",
        "button[aria-haspopup='menu']:has-text('Veo 3.1')",
        "button[aria-haspopup='menu']:has-text('Veo 3')",
        "button[aria-haspopup='menu']:has-text('Veo')",
        "button[aria-haspopup='menu']:has-text('Nano Banana 2')",
        "button:has-text('Veo 3.1 - Fast')",
        "button:has-text('Veo 3.1')",
        "button:has-text('Veo 3')",
        "button:has-text('Veo3')",
        "button:has-text('Nano Banana 2')",
    )
    for sel in open_selectors:
        try:
            btn = page.locator(sel).first
            if btn.count() > 0 and btn.is_visible(timeout=900) and _safe_click(btn):
                page.wait_for_timeout(450)
                try:
                    page.locator("[role='menu'][data-state='open']").first.wait_for(state="visible", timeout=1200)
                except Exception:
                    pass
                opened = True
                break
        except Exception:
            continue

    choices = _discover_flow_model_choices(page) if opened else []
    if choices:
        try:
            _save_flow_model_cache(choices)
        except Exception:
            pass
    target = _best_model_match(requested_model, choices)
    labels = [x for x in (target, requested_model, "Veo 3.1", "Veo 3", "Veo3") if str(x).strip()]
    for label in labels:
        try:
            btn = page.locator(f"[role='menuitem']:has-text('{label}'), [role='option']:has-text('{label}'), button:has-text('{label}')").first
            if btn.count() > 0 and btn.is_visible(timeout=900) and _safe_click(btn):
                page.wait_for_timeout(500)
                if requested_model:
                    logger.info("Flow model: requested='{}' -> selected='{}'", requested_model, label)
                return
        except Exception:
            continue
    logger.warning("Không chọn được model Veo từ UI (requested='{}').", requested_model or "(trống)")


def _mode_button_text(page: Page) -> str:
    """
    Đọc text nút mode/settings gần composer (thường có dạng ``Video ... x2``).
    """
    selectors = [
        "button[aria-haspopup='menu']",
        "button:has-text('Video')",
        "button:has-text('Nano Banana')",
    ]
    for sel in selectors:
        try:
            loc = page.locator(sel).last
            if loc.count() == 0 or not loc.is_visible(timeout=800):
                continue
            txt = str(loc.inner_text(timeout=1200) or "").strip()
            if txt:
                return txt
        except Exception:
            continue
    return ""


def _ensure_video_mode_selected(page: Page) -> None:
    """
    Đảm bảo mode đang là ``Video``:
    - nếu nút mode đã chứa text ``Video`` thì giữ nguyên
    - nếu chưa, mở menu và chọn lại ``Video``
    """
    current = _mode_button_text(page)
    if re.search(r"\bVideo\b", current, flags=re.I):
        return

    # Mở menu mode/settings
    opened = False
    for sel in (
        "button[aria-haspopup='menu']:has-text('Nano Banana')",
        "button[aria-haspopup='menu']",
        "button:has-text('Nano Banana')",
        "button:has-text('Video')",
    ):
        try:
            btn = page.locator(sel).last
            if btn.count() == 0 or not btn.is_visible(timeout=800):
                continue
            btn.click(timeout=2500)
            page.wait_for_timeout(700)
            opened = True
            break
        except Exception:
            continue

    if not opened:
        return

    # Chọn lại Video trong menu.
    for sel in (
        "button:has-text('Video')",
        "[role='menuitem']:has-text('Video')",
        "text=/\\bVideo\\b/i",
    ):
        try:
            opt = page.locator(sel).first
            if opt.count() == 0 or not opt.is_visible(timeout=1000):
                continue
            opt.click(timeout=2500)
            page.wait_for_timeout(900)
            break
        except Exception:
            continue


def _ensure_frames_tab_selected(page: Page) -> None:
    """
    Đảm bảo tab ``Khung hình`` (VIDEO_FRAMES) được chọn sau khi vào mode Video.
    """
    for sel in (
        "button[role='tab'][aria-controls*='VIDEO_FRAMES'][aria-selected='true']",
        "button[role='tab'][data-state='active']:has-text('Khung hình')",
        "button[role='tab'][aria-selected='true']:has-text('Khung hình')",
    ):
        try:
            loc = page.locator(sel).first
            if loc.count() > 0 and loc.is_visible(timeout=700):
                return
        except Exception:
            continue

    for sel in (
        "button[role='tab'][aria-controls*='VIDEO_FRAMES']",
        "button[role='tab']:has-text('Khung hình')",
        "button[role='tab']:has-text('Frames')",
    ):
        try:
            tab = page.locator(sel).first
            if tab.count() == 0 or not tab.is_visible(timeout=1000):
                continue
            tab.click(timeout=2500)
            page.wait_for_timeout(700)
            try:
                if str(tab.get_attribute("aria-selected") or "").lower() == "true":
                    return
            except Exception:
                pass
        except Exception:
            continue


def select_text_to_video_mode_if_needed(page: Page, requested_model: str = "") -> None:
    """
    Chọn Text-to-Video / Veo nếu UI yêu cầu.
    Nếu đã có prompt box thì không bắt buộc chọn mode.
    """
    if has_prompt_box(page):
        _ensure_video_mode_selected(page)
        _ensure_frames_tab_selected(page)
        _try_select_veo3_model(page, requested_model=requested_model)
        return

    for sel in (
        "button:has-text('Text to Video')",
        "button:has-text('Từ văn bản sang video')",
        "button:has-text('From text to video')",
    ):
        try:
            btn = page.locator(sel).first
            if btn.is_visible(timeout=2000):
                btn.click(timeout=3000)
                page.wait_for_timeout(2000)
                _try_select_veo3_model(page, requested_model=requested_model)
                if has_prompt_box(page):
                    return
        except Exception:
            continue

    try:
        mode = page.get_by_text(_FLOW_MODE_NAMES).first
        mode.wait_for(state="visible", timeout=8000)
        mode.click(timeout=3000)
        page.wait_for_timeout(2000)
    except PlaywrightTimeoutError:
        pass
    except Exception:
        pass

    _try_select_veo3_model(page, requested_model=requested_model)
    _ensure_video_mode_selected(page)
    _ensure_frames_tab_selected(page)

    if not has_prompt_box(page):
        raise RuntimeError("Không vào được mode Text-to-Video hoặc không thấy prompt box.")


def split_text_chunks(text: str, max_len: int = 700) -> list[str]:
    """Chia prompt dài theo dòng để nhập ổn định."""
    chunks: list[str] = []
    current = ""
    for line in str(text or "").splitlines(True):
        if len(current) + len(line) > max_len:
            if current:
                chunks.append(current)
            current = line
        else:
            current += line
    if current:
        chunks.append(current)
    return chunks if chunks else [""]


def input_prompt_to_flow(page: Page, final_prompt: str) -> None:
    """Nhập prompt vào Flow: ``press_sequentially``, không inject DOM."""
    prompt_box = find_flow_prompt_box(page)
    prompt_box.click(timeout=3000)
    page.wait_for_timeout(random.randint(500, 1200))
    try:
        prompt_box.press("Control+A")
        page.wait_for_timeout(200)
        prompt_box.press("Backspace")
    except Exception:
        pass
    page.wait_for_timeout(random.randint(500, 1000))
    for chunk in split_text_chunks(final_prompt, 700):
        prompt_box.press_sequentially(chunk, delay=random.randint(8, 25))
        page.wait_for_timeout(random.randint(300, 800))


def wait_flow_generation_done(page: Page, *, timeout_ms: int = 30 * 60 * 1000) -> None:
    """
    Chờ video render xong ổn định: video + download + không còn generating.

    Poll ngắn hơn khi đã có video (Flow hay cập nhật chậm nếu chờ cố định 5s/vòng).
    Số tick ổn định có thể chỉnh qua VEO3_FLOW_STABLE_TICKS (mặc 2), chu kỳ qua VEO3_FLOW_DONE_POLL_MS (mặc 900).
    """
    deadline = time.time() + max(60.0, timeout_ms / 1000.0)
    download_names = re.compile(r"Download|Tải xuống|Save|Lưu", re.I)
    stable_need = max(1, min(5, int(float(os.environ.get("VEO3_FLOW_STABLE_TICKS", "2") or "2"))))
    poll_base = max(300, min(5000, int(float(os.environ.get("VEO3_FLOW_DONE_POLL_MS", "900") or "900"))))
    stable_count = 0
    while time.time() < deadline:
        has_video = False
        has_download = False
        still_generating = False
        try:
            if page.locator("video").count() > 0:
                video = page.locator("video").first
                if video.is_visible(timeout=1000):
                    has_video = True
        except Exception:
            pass
        try:
            download_btn = page.get_by_role("button", name=download_names).first
            if download_btn.is_visible(timeout=1000):
                has_download = True
        except Exception:
            pass
        try:
            tx = page.locator("text=/Generating|Đang tạo|Creating|Rendering|Processing/i").first
            if tx.is_visible(timeout=400):
                still_generating = True
        except Exception:
            pass
        try:
            pb = page.locator("[role='progressbar']").first
            if pb.is_visible(timeout=200):
                still_generating = True
        except Exception:
            pass
        try:
            busy = page.locator("[aria-busy='true']").first
            if busy.is_visible(timeout=200):
                still_generating = True
        except Exception:
            pass
        if has_video and has_download and not still_generating:
            stable_count += 1
        else:
            stable_count = 0
        if stable_count >= stable_need:
            return
        if has_video and has_download:
            sleep_ms = min(1600, poll_base)
        elif has_video:
            sleep_ms = min(2400, int(poll_base * 1.35))
        else:
            sleep_ms = min(4000, int(poll_base * 2.4))
        page.wait_for_timeout(sleep_ms)
    raise RuntimeError("Timeout khi chờ video tạo xong.")


def run_google_flow_create_project_and_generate(
    page: Page,
    final_prompt: str,
    *,
    skip_initial_navigation: bool = False,
) -> None:
    """
    Mở Flow (tùy chọn), kiểm tra login, tạo project, chọn mode, nhập prompt, generate, chờ xong.
    """
    if not skip_initial_navigation:
        page.goto(GOOGLE_FLOW_URL, wait_until="domcontentloaded", timeout=60_000)
    page.wait_for_timeout(3000)
    if not check_google_flow_login(page):
        raise RuntimeError("Google Flow chưa đăng nhập. Cần user login thủ công.")
    open_or_create_flow_project(page)
    select_text_to_video_mode_if_needed(page)
    input_prompt_to_flow(page, final_prompt)
    click_generate_and_wait_started(
        page,
        screenshot_prefix="google_flow_create_project_generate",
        generation_started_timeout_ms=30_000,
        include_video_signal=True,
    )
    wait_flow_generation_done(page)


def _prompt_text_content(prompt_box: Locator) -> str:
    """Lấy nội dung thực tế của ô prompt (textarea hoặc contenteditable)."""
    try:
        return str(
            prompt_box.evaluate(
                """(el) => {
                    const tag = (el.tagName || '').toLowerCase();
                    if (tag === 'textarea' || tag === 'input')
                        return (el.value || '').trim();
                    return (el.innerText || el.textContent || '').trim();
                }"""
            )
            or ""
        ).strip()
    except Exception:
        return ""


def find_flow_prompt_box(page: Page) -> Locator:
    """
    Tìm ô nhập prompt trong project Google Flow.
    Ưu tiên placeholder Prompt / Describe / Mô tả / Create, sau đó ``.last`` theo spec.
    """
    placeholder_first = [
        "textarea[placeholder*='Prompt' i]",
        "textarea[placeholder*='Describe' i]",
        "textarea[placeholder*='Mô tả' i]",
        "textarea[placeholder*='Create' i]",
    ]
    for sel in placeholder_first:
        loc = page.locator(sel).last
        try:
            if loc.count() > 0:
                loc.wait_for(state="visible", timeout=5000)
                return loc
        except PlaywrightTimeoutError:
            continue
        except Exception:
            continue

    for sel in ("textarea", "[contenteditable='true']", "[role='textbox']", "div[contenteditable='true']"):
        loc = page.locator(sel).last
        try:
            if loc.count() > 0:
                loc.wait_for(state="visible", timeout=5000)
                return loc
        except PlaywrightTimeoutError:
            continue
        except Exception:
            continue

    selectors = [
        "[contenteditable='true'][role='textbox']",
        "[role='textbox'][contenteditable='true']",
        "[contenteditable='true']",
        "[role='textbox']",
        "textarea",
    ]
    for sel in selectors:
        loc = page.locator(sel)
        try:
            n = loc.count()
        except Exception:
            continue
        for idx in range(n - 1, -1, -1):
            cand = loc.nth(idx)
            try:
                if cand.is_visible(timeout=2000):
                    return cand
            except Exception:
                continue
    raise RuntimeError("Không tìm thấy ô nhập prompt trong project Google Flow.")


def verify_prompt_nonempty(prompt_box: Locator) -> None:
    """Xác nhận prompt đã có nội dung (không rỗng)."""
    if not _prompt_text_content(prompt_box):
        raise RuntimeError("Prompt box đang rỗng; không được bấm tạo video.")


def _last_visible_in(locator: Locator) -> Locator | None:
    """Chọn phần tử cuối cùng trong locator mà vẫn visible (thường là nút gửi trong composer)."""
    try:
        n = locator.count()
    except Exception:
        return None
    for i in range(n - 1, -1, -1):
        cand = locator.nth(i)
        try:
            if cand.is_visible(timeout=1200):
                return cand
        except Exception:
            continue
    return None


def _button_near_prompt(prompt_box: Locator) -> Locator | None:
    """
    Tìm button trong các tổ tiên gần của prompt (ưu tiên có text khớp nút tạo).
    """
    for level in range(2, 14):
        try:
            root = prompt_box.locator(f"xpath=ancestor::*[{level}]")
            if root.count() == 0:
                continue
            root = root.first
            scoped = root.locator("button").filter(has_text=_FLOW_BUTTON_NAMES)
            hit = _last_visible_in(scoped)
            if hit is not None:
                return hit
            for aria in (
                "button[aria-label*='Generate' i]",
                "button[aria-label*='Create' i]",
                "button[aria-label*='Submit' i]",
                "button[aria-label*='Send' i]",
                "button[aria-label*='Tạo' i]",
                "button[aria-label*='Gửi' i]",
            ):
                hit = _last_visible_in(root.locator(aria))
                if hit is not None:
                    return hit
            submit = _last_visible_in(root.locator("button[type='submit']"))
            if submit is not None:
                return submit
        except Exception:
            continue
    return None


def find_flow_generate_button(page: Page, prompt_box: Locator | None = None) -> Locator:
    """
    Tìm nút tạo video trên Google Flow.
    Thứ tự: role button + tên → aria-label → gần prompt box → button[type=submit].
    """
    logger.info("[INFO] Đang tìm nút tạo video")
    try:
        role_btns = page.get_by_role("button", name=_FLOW_BUTTON_NAMES)
        hit = _last_visible_in(role_btns)
        if hit is not None:
            logger.info("[INFO] Đã tìm thấy nút tạo video (role + tên)")
            return hit
    except Exception:
        pass

    aria_chain = page.locator(
        "button[aria-label*='Generate' i], "
        "button[aria-label*='Create' i], "
        "button[aria-label*='Submit' i], "
        "button[aria-label*='Send' i], "
        "button[aria-label*='Tạo' i], "
        "button[aria-label*='Gửi' i], "
        "button:has(i:has-text('arrow_forward'))"
    )
    hit = _last_visible_in(aria_chain)
    if hit is not None:
        logger.info("[INFO] Đã tìm thấy nút tạo video (aria-label)")
        return hit

    if prompt_box is not None:
        near = _button_near_prompt(prompt_box)
        if near is not None:
            logger.info("[INFO] Đã tìm thấy nút tạo video (gần prompt box)")
            return near

    hit = _last_visible_in(page.locator("button[type='submit']"))
    if hit is not None:
        logger.info("[INFO] Đã tìm thấy nút tạo video (type=submit)")
        return hit

    logger.error("[ERROR] Không tìm thấy nút tạo video")
    raise RuntimeError("Không tìm thấy nút tạo video trên Google Flow.")


def wait_generate_button_ready(button: Locator) -> None:
    """
    Đảm bảo nút tạo video visible, enabled và không bị chặn click thử (trial).
    """
    logger.info("[INFO] Kiểm tra nút tạo video sẵn sàng")
    button.wait_for(state="visible", timeout=15_000)
    aria_disabled = button.get_attribute("aria-disabled")
    if str(aria_disabled).lower() == "true":
        logger.error("[ERROR] Nút tạo video đang disabled")
        raise RuntimeError("Nút tạo video đang disabled (aria-disabled).")
    if button.get_attribute("disabled") is not None:
        logger.error("[ERROR] Nút tạo video đang disabled")
        raise RuntimeError("Nút tạo video đang disabled (disabled).")
    try:
        button.click(trial=True, timeout=4000)
    except Exception as exc:
        logger.error("[ERROR] Nút tạo video chưa sẵn sàng để click: {}", exc)
        raise RuntimeError(f"Nút tạo video chưa sẵn sàng để click: {exc}") from exc


def wait_generation_started(
    page: Page,
    *,
    timeout_ms: int = 20_000,
    include_video_signal: bool = False,
) -> None:
    """
    Chờ Google Flow chuyển sang trạng thái generating (text, busy, progressbar; tùy chọn thêm video).
    """
    logger.info("[INFO] Đang chờ Google Flow bắt đầu tạo video")
    deadline = time.time() + max(3.0, timeout_ms / 1000.0)
    while time.time() < deadline:
        if _has_generation_signal(page, include_video_signal=include_video_signal):
            return
        time.sleep(0.35)
    # Grace window: đôi lúc UI cập nhật chậm, kiểm tra thêm 1 nhịp trước khi fail.
    page.wait_for_timeout(1200)
    if _has_generation_signal(page, include_video_signal=include_video_signal):
        logger.info("[INFO] Xác nhận generating muộn (grace window).")
        return
    logger.error("[ERROR] Bấm nút tạo video xong nhưng không thấy trạng thái generating")
    raise RuntimeError("Không xác nhận được Google Flow đã bắt đầu tạo video.")


def _has_generation_signal(page: Page, *, include_video_signal: bool) -> bool:
    """Tổng hợp tín hiệu đang render để tránh fail giả trên UI Flow thay đổi."""
    try:
        # Nhiều UI Flow hiển thị % tiến trình (vd. 40%) thay vì progressbar chuẩn.
        pct = page.locator("text=/\\b\\d{1,3}%\\b/").first
        if pct.is_visible(timeout=220):
            return True
    except Exception:
        pass
    try:
        tx = page.locator("text=/Generating|Đang tạo|Creating|Rendering|Processing/i").first
        if tx.is_visible(timeout=260):
            return True
    except Exception:
        pass
    try:
        t = page.get_by_text(_GENERATION_TEXT).first
        if t.is_visible(timeout=260):
            return True
    except Exception:
        pass
    try:
        busy = page.locator("[aria-busy='true']").first
        if busy.is_visible(timeout=200):
            return True
    except Exception:
        pass
    try:
        prog = page.locator("[role='progressbar']").first
        if prog.is_visible(timeout=200):
            return True
    except Exception:
        pass
    if include_video_signal:
        try:
            vid = page.locator("video").first
            if vid.is_visible(timeout=260):
                return True
        except Exception:
            pass
    return False


def click_generate_and_wait_started(
    page: Page,
    *,
    max_attempts: int = 1,
    screenshot_prefix: str = "google_flow_generate",
    generation_started_timeout_ms: int = 20_000,
    include_video_signal: bool = False,
) -> None:
    """
    Luồng: UI ổn định → tìm prompt → validate → tìm nút → chờ ready → click 1 lần
    → chờ generating. Mỗi lần retry re-find locator; tối đa max_attempts lần.
    """
    last_error: Exception | None = None
    for attempt in range(max(1, max_attempts)):
        try:
            dismiss_obvious_overlays(page)
            prompt_box = find_flow_prompt_box(page)
            verify_prompt_nonempty(prompt_box)
            logger.info("[INFO] Đã nhập prompt vào Google Flow")

            delay_ms = random.randint(1000, 2000)
            page.wait_for_timeout(delay_ms)

            button = find_flow_generate_button(page, prompt_box)
            wait_generate_button_ready(button)

            page.wait_for_timeout(random.randint(500, 1200))
            logger.info("[INFO] Bấm nút tạo video")
            button.click(timeout=6000)

            wait_generation_started(
                page,
                timeout_ms=generation_started_timeout_ms,
                include_video_signal=include_video_signal,
            )
            logger.info("[SUCCESS] Google Flow đã bắt đầu tạo video")
            return
        except PlaywrightTimeoutError as exc:
            last_error = exc
            logger.warning("[WARN] Lần {} bấm nút tạo video (timeout): {}", attempt + 1, exc)
        except Exception as exc:
            last_error = exc
            logger.warning("[WARN] Lần {} bấm nút tạo video: {}", attempt + 1, exc)
        page.wait_for_timeout(random.randint(1000, 2000))

    _save_failure_screenshot(page, reason=f"{screenshot_prefix}_failed")
    msg = f"Bấm nút tạo video thất bại sau {max_attempts} lần thử"
    if last_error is not None:
        msg = f"{msg}: {last_error}"
    raise RuntimeError(msg) from last_error


def run_generate_step(page: Page, *, screenshot_prefix: str = "google_flow_run_generate") -> None:
    """
    Bước bấm nút tạo video trong Google Flow (sau khi prompt đã nhập xong ở bước trước).
    """
    click_generate_and_wait_started(page, screenshot_prefix=screenshot_prefix)


def is_google_flow_labs_url(url: str) -> bool:
    """True nếu URL trỏ tới Google Labs Flow / fx."""
    u = (url or "").lower()
    return "labs.google" in u or "/fx/" in u
