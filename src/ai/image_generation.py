"""
Sinh ảnh minh họa bài đăng (Google Imagen qua Gemini API / ``google-genai``).

Cần ``GEMINI_API_KEY``. Model mặc định: ``imagen-3.0-generate-002`` (ghi đè bằng ``GEMINI_IMAGE_MODEL``).
"""

from __future__ import annotations

import base64
from datetime import datetime
import hashlib
import os
import random
import re
import uuid
import time
from pathlib import Path
from io import BytesIO
from typing import Any, Literal

from loguru import logger
import requests
from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

from src.utils.page_workspace import ensure_page_workspace
from src.utils.paths import project_root


def _human_pause(min_sec: float = 0.8, max_sec: float = 1.6) -> None:
    """
    Delay kiểu người dùng thật giữa các thao tác UI.
    Có thể cấu hình bằng NANOBANANA_ACTION_DELAY_MS (mili giây).
    """
    raw = os.environ.get("NANOBANANA_ACTION_DELAY_MS", "").strip()
    if raw:
        try:
            v = max(120, int(float(raw)))
            time.sleep(v / 1000.0)
            return
        except Exception:
            pass
    lo = max(0.05, float(min_sec))
    hi = max(lo, float(max_sec))
    time.sleep(random.uniform(lo, hi))


def _nb_status(tag: str, message: str) -> None:
    """
    Log trạng thái ngắn gọn cho luồng tạo ảnh NanoBanana browser.
    """
    logger.info("[NB:{}] {}", tag, message)


def _img_status(tag: str, message: str) -> None:
    """
    Log tổng quát cho pipeline sinh ảnh (provider-agnostic).
    """
    logger.info("[IMG:{}] {}", tag, message)


def _fast_chromium_args(*, locale_lang: str, direct_no_proxy: bool) -> list[str]:
    _ = direct_no_proxy
    # Giữ tối thiểu để tránh crash do profile/flag conflict trên một số máy.
    return [f"--lang={locale_lang}"]


def _launch_nb_context(pw, *, profile_dir: Path):
    """
    Ưu tiên Chrome hệ thống cho tốc độ tải như browser ngoài.
    Fallback Chromium mặc định của Playwright nếu máy không có Chrome.
    """
    keep_system_proxy = str(os.environ.get("NANOBANANA_KEEP_SYSTEM_PROXY", "")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    kwargs = dict(
        user_data_dir=str(profile_dir),
        headless=False,
        viewport={"width": 1366, "height": 900},
        locale="en-US",
        args=_fast_chromium_args(locale_lang="en-US", direct_no_proxy=not keep_system_proxy),
    )
    channel = str(os.environ.get("NANOBANANA_CHROMIUM_CHANNEL", "chrome")).strip().lower()
    if channel and channel not in {"bundled", "none", "0", "false", "off"}:
        try:
            return pw.chromium.launch_persistent_context(channel=channel, **kwargs)
        except Exception as exc:
            logger.warning("NanoBanana launch channel={} lỗi, fallback bundle: {}", channel, exc)
    try:
        return pw.chromium.launch_persistent_context(**kwargs)
    except Exception as exc:
        # Fallback cuối: profile sạch để tránh profile cũ bị corrupt/lock làm chết browser ngay lúc launch.
        clean_profile = profile_dir.parent / f"{profile_dir.name}_clean"
        clean_profile.mkdir(parents=True, exist_ok=True)
        logger.warning(
            "NanoBanana launch profile chính lỗi, thử profile sạch: {} | err={}",
            clean_profile,
            exc,
        )
        clean_kwargs = dict(kwargs)
        clean_kwargs["user_data_dir"] = str(clean_profile)
        return pw.chromium.launch_persistent_context(**clean_kwargs)


def _open_fresh_page(context, *, target_url: str):
    """
    Chọn tab an toàn để điều hướng, hạn chế thao tác đóng/mở tab gây lỗi pipe/protocol.
    """
    page = None
    try:
        for pg in list(context.pages):
            try:
                if not pg.is_closed():
                    page = pg
                    break
            except Exception:
                continue
    except Exception:
        page = None
    if page is None:
        page = context.new_page()
    # Điều hướng nhẹ nhàng, tránh block quá sớm ở domcontentloaded/load.
    page.goto(target_url, wait_until="commit", timeout=60_000)
    return page


def _translate_text_to_english(text: str, *, max_chars: int = 1800) -> str:
    """
    Dịch nhanh văn bản sang tiếng Anh để dùng làm prompt ảnh.
    Nếu không có GEMINI_API_KEY hoặc lỗi mạng/API thì trả nguyên bản.
    """
    raw = str(text or "").strip()
    if not raw:
        return ""
    if not os.environ.get("GEMINI_API_KEY", "").strip():
        return raw
    try:
        from google import genai
    except Exception:
        return raw
    try:
        model = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash").strip() or "gemini-2.5-flash"
        client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY", "").strip())
        req = (
            "Translate the following text into natural English for an image-generation prompt. "
            "Keep key entities and context. Return only English text, no explanation.\n\n"
            f"{raw[:max_chars]}"
        )
        resp = client.models.generate_content(model=model, contents=req)
        out = str(getattr(resp, "text", "") or "").strip()
        return out or raw
    except Exception:
        return raw


def suggest_image_style_from_post(
    *,
    title: str,
    body: str,
    language_hint: str = "English",
    timeout_seconds: float = 18.0,
) -> str:
    """
    Đề xuất 1 câu ngắn mô tả **phong cách ảnh** (tiếng Anh) dựa trên tiêu đề + nội dung bài viết.

    Dùng khi user chọn «Auto» ở batch dialog: mỗi bài sẽ có style riêng khớp chủ đề/cảm xúc.
    Trả về chuỗi rỗng nếu thiếu ``GEMINI_API_KEY`` hoặc gọi model lỗi — caller sẽ fallback về style mặc định.
    """
    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not api_key:
        return ""
    try:
        import google.generativeai as genai  # noqa: WPS433
    except Exception:
        return ""
    tit = (title or "").strip()[:240]
    bod = re.sub(r"\s+", " ", (body or "").strip())[:600]
    if not tit and not bod:
        return ""
    model_name = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash").strip() or "gemini-2.5-flash"
    prompt = (
        "You are an art director for social media imagery. "
        "Given a post title and excerpt, propose ONE short image style line (English), "
        "maximum 18 words, describing composition, lighting, mood, palette suitable for that post. "
        "Do NOT include topic/subject — only stylistic directives. "
        "Do NOT include hashtags, quotes, emoji, or trailing punctuation. "
        "Return plain text only (no JSON, no markdown).\n\n"
        f"Title: {tit}\n"
        f"Excerpt: {bod}\n"
        f"Preferred description language: {language_hint or 'English'}\n"
        "Style:"
    )
    try:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel(model_name)
        resp = model.generate_content(
            prompt,
            generation_config={"temperature": 0.6, "max_output_tokens": 80},
            request_options={"timeout": float(max(3.0, timeout_seconds))},
        )
        out = (getattr(resp, "text", "") or "").strip()
    except Exception:
        return ""
    out = out.split("\n", 1)[0].strip().strip('"').strip("'").rstrip(".,;:")
    if len(out) > 220:
        out = out[:220].rsplit(" ", 1)[0]
    return out


def build_imagen_prompt_from_post(
    *,
    title: str,
    body: str,
    image_style: str = "",
) -> str:
    """Prompt tiếng Anh cho Imagen, dựa trên tiêu đề + trích nội dung bài (câu chuyện)."""
    # Rút gọn nội dung chính, loại lặp để tránh prompt dài/double.
    clean_body = re.sub(r"\s+", " ", (body or "").strip())
    chunks = [x.strip() for x in re.split(r"[.!?]+", clean_body) if x.strip()]
    dedup: list[str] = []
    seen: set[str] = set()
    for c in chunks:
        key = c.lower()[:90]
        if key in seen:
            continue
        seen.add(key)
        dedup.append(c)
        if len(" ".join(dedup)) >= 320:
            break
    excerpt_src = " ".join(dedup)[:320]
    style_src = (image_style or "").strip() or "modern social media illustration, vibrant, clean composition"
    tit_src = (title or "").strip()[:220]
    excerpt = _translate_text_to_english(excerpt_src, max_chars=600)
    style = _translate_text_to_english(style_src, max_chars=260)
    tit = _translate_text_to_english(tit_src, max_chars=320)
    return (
        "Create one square 1:1 image. "
        f"Topic: {tit}. "
        f"Key context: {excerpt}. "
        f"Style: {style}. "
        "No watermark, no logo, no text overlay."
    )[:900]


def _imagen_model_candidates(explicit: str | None) -> list[str]:
    out: list[str] = []
    if explicit and explicit.strip():
        out.append(explicit.strip())
    env = os.environ.get("GEMINI_IMAGE_MODEL", "").strip()
    if env and env not in out:
        out.append(env)
    for m in ("imagen-3.0-generate-002", "imagen-3.0-generate-001"):
        if m not in out:
            out.append(m)
    return out


def _canonical_nano_banana_pro_gemini_model(model_id: str | None) -> str | None:
    """
    Chuẩn hóa tên model marketing (nano-banana-pro) sang model ID Gemini API thực tế.
    Nano Banana Pro = Gemini 3 Pro Image (``generate_content``, không dùng Imagen ``generate_images``).
    """
    if not model_id or not str(model_id).strip():
        return None
    m = str(model_id).strip().lower()
    aliases = {
        "nano-banana-pro": "gemini-3-pro-image-preview",
        "nano_banana_pro": "gemini-3-pro-image-preview",
        "nanobananapro": "gemini-3-pro-image-preview",
        # Một số tài liệu / env dùng tên cũ; map sang model ID hiện hành.
        "gemini-2.5-flash-preview-image": "gemini-2.5-flash-image",
    }
    if m in aliases:
        return aliases[m]
    return str(model_id).strip()


def _nano_banana_pro_model_candidates(explicit: str | None) -> list[str]:
    """Thứ tự ưu tiên model Gemini cho luồng Nano Banana Pro (ảnh native)."""
    out: list[str] = []
    can = _canonical_nano_banana_pro_gemini_model(explicit)
    if can:
        out.append(can)
    env_raw = os.environ.get("NANO_BANANA_PRO_MODEL", "").strip()
    env_resolved = _canonical_nano_banana_pro_gemini_model(env_raw) or env_raw
    if env_resolved and env_resolved not in out:
        out.append(env_resolved)
    for m in (
        "gemini-3-pro-image-preview",
        "gemini-3.1-flash-image-preview",
        "gemini-2.5-flash-image",
    ):
        if m not in out:
            out.append(m)
    return out


def _parse_retry_delay_seconds_from_google_api(exc: BaseException) -> float | None:
    """Trích số giây từ chuỗi kiểu ``retry in 17.85s`` trong message lỗi Google API."""
    raw = str(exc)
    m = re.search(r"retry in\s+([0-9]+(?:\.[0-9]+)?)\s*s", raw, re.IGNORECASE)
    if not m:
        return None
    try:
        val = float(m.group(1))
        return val if val > 0 else None
    except ValueError:
        return None


def _is_quota_or_rate_limit_error(exc: BaseException) -> bool:
    """True nếu lỗi có vẻ 429 / quota / RESOURCE_EXHAUSTED (Google GenAI)."""
    raw = str(exc).lower()
    if "429" in raw or "resource_exhausted" in raw:
        return True
    code = getattr(exc, "status_code", None) or getattr(exc, "code", None)
    if code == 429:
        return True
    if "quota" in raw and ("exceed" in raw or "exhaust" in raw):
        return True
    return False


def _nano_banana_pro_inter_image_delay_sec() -> float:
    """Khoảng nghỉ giữa hai lần ``generate_content`` khi sinh nhiều ảnh (tránh burst free tier)."""
    raw = (os.environ.get("NANO_BANANA_PRO_INTER_IMAGE_DELAY_SEC", "") or "2.5").strip()
    try:
        base = float(raw)
    except ValueError:
        base = 2.5
    return max(0.5, min(30.0, base)) + random.uniform(0.35, 1.15)


def _nano_banana_pro_quota_hint_suffix(last_exc: BaseException | None) -> str:
    """Thêm gợi ý tiếng Việt khi lỗi cuối liên quan quota / 429."""
    if last_exc is None:
        return ""
    s = str(last_exc).lower()
    if not ("resource_exhausted" in s or "quota" in s or "429" in s):
        return ""
    return (
        " Gợi ý: kiểm tra billing / gói trả phí cho project chứa GEMINI_API_KEY; "
        "free tier có thể không có quota model ảnh. Thử giảm số ảnh hoặc chờ reset quota theo ngày."
    )


def _image_bytes_list_from_generate_content_response(resp: Any) -> list[bytes]:
    """Trích các ảnh (bytes) từ phản hồi ``generate_content`` của model image Gemini."""
    blobs: list[bytes] = []
    parts: list[Any] = []
    if resp is None:
        return blobs
    if getattr(resp, "parts", None):
        parts = list(resp.parts)
    else:
        for cand in getattr(resp, "candidates", None) or []:
            cont = getattr(cand, "content", None)
            if cont is not None and getattr(cont, "parts", None):
                parts.extend(list(cont.parts))
    for part in parts:
        inline = getattr(part, "inline_data", None) or getattr(part, "inlineData", None)
        data = None
        if inline is not None:
            data = getattr(inline, "data", None)
            if data is None and isinstance(inline, dict):
                data = inline.get("data")
        if isinstance(data, bytes) and data:
            blobs.append(data)
            continue
        if isinstance(data, str) and data.strip():
            try:
                raw = base64.b64decode(data, validate=False)
                if raw:
                    blobs.append(raw)
                    continue
            except Exception:
                pass
        as_img = getattr(part, "as_image", None)
        if callable(as_img):
            try:
                im = as_img()
                buf = BytesIO()
                im.save(buf, format="PNG")
                raw = buf.getvalue()
                if raw:
                    blobs.append(raw)
            except Exception:
                pass
    return blobs


def generate_post_images_nano_banana_pro(
    *,
    prompt: str,
    number_of_images: int = 1,
    api_key: str | None = None,
    model: str | None = None,
    max_retries: int = 3,
) -> list[bytes]:
    """
    Sinh ảnh qua Gemini API — Nano Banana Pro (Gemini 3 Pro Image): ``models.generate_content`` + ảnh inline.

    Raises:
        RuntimeError: thiếu key, thiếu gói, hoặc API lỗi.
    """
    api_key = (api_key or os.environ.get("GEMINI_API_KEY", "")).strip()
    if not api_key:
        raise RuntimeError("Thiếu GEMINI_API_KEY để sinh ảnh (Nano Banana Pro).")
    try:
        from google import genai
        from google.genai import types
    except ImportError as exc:
        raise RuntimeError("Cần cài google-genai: pip install google-genai") from exc

    n = max(1, min(8, int(number_of_images)))
    retries = max(1, min(5, int(max_retries)))
    base_prompt = (prompt or "")[:4500]
    last_exc: Exception | None = None
    for attempt in range(retries):
        client = genai.Client(api_key=api_key)
        for mname in _nano_banana_pro_model_candidates(model):
            blobs: list[bytes] = []
            try:
                try:
                    img_cfg = types.GenerateContentConfig(
                        response_modalities=["TEXT", "IMAGE"],
                    )
                except Exception:
                    img_cfg = None
                _pc_raw = (os.environ.get("NANO_BANANA_PRO_PER_CALL_RETRIES", "") or "6").strip() or "6"
                try:
                    _pc_n = int(_pc_raw)
                except ValueError:
                    _pc_n = 6
                per_call_max = max(1, min(12, _pc_n))
                for i in range(n):
                    if n == 1:
                        contents = base_prompt
                    else:
                        contents = (
                            f"{base_prompt}\n\n"
                            f"[Output {i + 1} of {n}] Generate one square portrait image only. "
                            "Vary pose or framing slightly while preserving the same character identity."
                        )
                    last_call_exc: BaseException | None = None
                    got_image = False
                    for pc in range(per_call_max):
                        try:
                            if img_cfg is not None:
                                resp = client.models.generate_content(
                                    model=mname,
                                    contents=contents,
                                    config=img_cfg,
                                )
                            else:
                                resp = client.models.generate_content(model=mname, contents=contents)
                            chunk = _image_bytes_list_from_generate_content_response(resp)
                            if not chunk:
                                raise RuntimeError(
                                    f"Model {mname} không trả về dữ liệu ảnh (lần {i + 1}/{n})."
                                )
                            blobs.append(chunk[0])
                            got_image = True
                            break
                        except Exception as call_exc:  # noqa: BLE001
                            last_call_exc = call_exc
                            if _is_quota_or_rate_limit_error(call_exc):
                                delay = _parse_retry_delay_seconds_from_google_api(call_exc)
                                if delay is None:
                                    delay = min(60.0, 2.0 * (2**pc))
                                delay = min(120.0, max(1.0, delay + random.uniform(0.15, 0.85)))
                                logger.warning(
                                    "Nano Banana Pro 429/quota model={} chờ {:.1f}s (thử {}/{} ảnh {}/{}).",
                                    mname,
                                    delay,
                                    pc + 1,
                                    per_call_max,
                                    i + 1,
                                    n,
                                )
                                time.sleep(delay)
                                continue
                            raise
                    if not got_image:
                        raise RuntimeError(
                            f"Model {mname} thất bại sau {per_call_max} lần thử (ảnh {i + 1}/{n})."
                        ) from last_call_exc
                    if i < n - 1:
                        gap = _nano_banana_pro_inter_image_delay_sec()
                        logger.debug("Nano Banana Pro nghỉ {:.1f}s trước ảnh tiếp theo.", gap)
                        time.sleep(gap)
                if len(blobs) >= n:
                    logger.info("Nano Banana Pro OK model={} ({} ảnh, generate_content).", mname, len(blobs))
                    _img_status("NANO_BANANA_PRO_OK", f"model={mname}, images={len(blobs)}")
                    return blobs
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                logger.warning("Nano Banana Pro model {} (attempt {}): {}", mname, attempt + 1, exc)
                continue
    hint = _nano_banana_pro_quota_hint_suffix(last_exc)
    raise RuntimeError(
        f"Nano Banana Pro lỗi sau {retries} lần thử. Lỗi cuối: {last_exc!r}.{hint}"
    ) from last_exc


def _nanobanana_model() -> str:
    return os.environ.get("NANOBANANA_MODEL", "nano-banana-pro").strip() or "nano-banana-pro"


def _nanobanana_url() -> str:
    return (
        os.environ.get("NANOBANANA_API_URL", "https://api.nanobananaapi.ai/api/v1/nanobanana/generate").strip()
        or "https://api.nanobananaapi.ai/api/v1/nanobanana/generate"
    )


def _nanobanana_record_info_url(task_id: str) -> str:
    base = os.environ.get("NANOBANANA_RECORD_INFO_URL", "").strip()
    if base:
        if "{task_id}" in base:
            return base.replace("{task_id}", task_id)
        if "taskId=" in base:
            return base
        sep = "&" if "?" in base else "?"
        return f"{base}{sep}taskId={task_id}"
    return f"https://api.nanobananaapi.ai/api/v1/nanobanana/record-info?taskId={task_id}"


def _nanobanana_callback_url() -> str:
    return os.environ.get("NANOBANANA_CALLBACK_URL", "https://example.com/nanobanana-callback").strip()


def _nanobanana_browser_enabled() -> bool:
    raw = os.environ.get("NANOBANANA_USE_BROWSER", "1").strip().lower()
    return raw not in {"0", "false", "off", "no"}


def _nanobanana_browser_strict() -> bool:
    """
    True = bắt buộc sinh ảnh bằng browser profile đã đăng nhập, không fallback sang API.
    """
    raw = os.environ.get("NANOBANANA_BROWSER_STRICT", "1").strip().lower()
    return raw not in {"0", "false", "off", "no"}


def _nanobanana_browser_profile_dir() -> Path:
    p = os.environ.get("NANOBANANA_BROWSER_PROFILE_DIR", "").strip()
    if p:
        d = Path(p)
    else:
        d = project_root() / "data" / "nanobanana" / "browser_profile"
    d.mkdir(parents=True, exist_ok=True)
    return d.resolve()


def _nanobanana_locked_ui_mode() -> bool:
    """
    Chế độ fix cứng UI để dùng ổn định trên nhiều máy.
    """
    raw = os.environ.get("NANOBANANA_LOCKED_UI", "1").strip().lower()
    return raw not in {"0", "false", "off", "no"}


def _locked_prompt_selectors() -> list[str]:
    return [
        "textarea[aria-label*='message' i]",
        "textarea[aria-label*='prompt' i]",
        "textarea",
        "[role='textbox'][contenteditable='true']",
    ]


def _locked_submit_selectors() -> list[str]:
    return [
        "button.send-button.submit[aria-label='Send message'][aria-disabled='false']",
        "button.send-button[aria-label='Send message'][aria-disabled='false']",
        "button.send-button.submit[aria-label='Send message']",
        "button.send-button[aria-label='Send message']",
        "button[aria-label='Submit']",
        "button[aria-label='Send message']",
        "button[mattooltip='Submit']",
        "button[title='Submit']",
        "[aria-label='Submit']",
        "[aria-label='Send message']",
        "button:has-text('Submit')",
        "[data-testid*='submit' i]",
    ]


def open_nanobanana_login_browser(*, wait_sec: int = 180) -> dict[str, str]:
    """
    Mở trình duyệt profile riêng để user đăng nhập Gemini Image/Nano Banana thủ công.

    Returns:
        Dict chứa ``url``/``title`` hiện tại sau khi chờ.
    """
    web_url = (
        os.environ.get("NANOBANANA_WEB_URL", "https://gemini.google.com/app?hl=en").strip()
        or "https://gemini.google.com/app?hl=en"
    )
    profile_dir = _nanobanana_browser_profile_dir()
    pw = sync_playwright().start()
    context = None
    try:
        t0 = time.perf_counter()
        context = _launch_nb_context(pw, profile_dir=profile_dir)
        t1 = time.perf_counter()
        try:
            stealth = Stealth()
            stealth.apply_stealth_sync(context)
        except Exception:
            pass
        page = _open_fresh_page(context, target_url=web_url)
        t2 = time.perf_counter()
        logger.info("Mở browser login NanoBanana/Gemini tại {} (profile={})", web_url, profile_dir)
        logger.info("NanoBanana timing: launch={:.2f}s, first-commit={:.2f}s", (t1 - t0), (t2 - t1))
        # wait_sec <= 0: chờ đến khi user tự đóng browser (không auto-close).
        no_auto_close = int(wait_sec) <= 0
        deadline = (time.time() + max(15, int(wait_sec))) if not no_auto_close else None
        while True:
            try:
                if page.is_closed() or len(context.pages) == 0:
                    break
            except Exception:
                break
            if (deadline is not None) and (time.time() >= deadline):
                break
            time.sleep(0.5)
        info = {"url": "", "title": ""}
        try:
            if not page.is_closed():
                info = {"url": page.url, "title": page.title()}
        except Exception:
            pass
        if int(wait_sec) > 0 and context is not None:
            try:
                context.close()
            except Exception:
                pass
        return info
    finally:
        # Grace period giảm xác suất EPIPE khi driver Node đang flush event.
        time.sleep(0.2)
        try:
            pw.stop()
        except Exception:
            pass


def _nanobanana_debug_screenshot(page, reason: str) -> Path | None:
    """
    Chụp màn hình debug khi automation NanoBanana thất bại (để khóa selector nhanh).
    """
    try:
        d = project_root() / "logs" / "screenshots"
        d.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        name = re.sub(r"[^a-zA-Z0-9_-]+", "_", reason).strip("_")[:48] or "nb_err"
        p = d / f"nanobanana_{name}_{ts}.png"
        page.screenshot(path=str(p), full_page=True)
        logger.warning("Đã chụp ảnh debug NanoBanana: {}", p)
        return p
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không chụp được ảnh debug NanoBanana: {}", exc)
        return None


def _nanobanana_api_keys() -> list[str]:
    """
    Trả về danh sách key NanoBanana khả dụng.
    Hỗ trợ:
    - NANOBANANA_API_KEYS="k1,k2,..."
    - NANOBANANA_API_KEY="k1"
    - alias theo yêu cầu user: VEO3_API_KEYS / VEO3_API_KEY
    """
    keys: list[str] = []
    multi = os.environ.get("NANOBANANA_API_KEYS", "").strip() or os.environ.get("VEO3_API_KEYS", "").strip()
    if multi:
        keys.extend([x.strip() for x in multi.split(",") if x.strip()])
    single = os.environ.get("NANOBANANA_API_KEY", "").strip() or os.environ.get("VEO3_API_KEY", "").strip()
    if single:
        keys.append(single)
    # unique giữ thứ tự
    out: list[str] = []
    seen: set[str] = set()
    for k in keys:
        if k not in seen:
            out.append(k)
            seen.add(k)
    return out


def _download_image_bytes(url: str, *, timeout_sec: int = 90) -> bytes:
    resp = requests.get(url, timeout=timeout_sec)
    resp.raise_for_status()
    return resp.content


def _pollinations_url(prompt: str, *, seed: int | None = None) -> str:
    """
    Endpoint text-to-image không cần API key.
    Docs/community phổ biến: https://image.pollinations.ai/prompt/<text>?...
    """
    from urllib.parse import quote_plus

    q = quote_plus(prompt[:1200])
    s = f"&seed={int(seed)}" if seed is not None else ""
    return f"https://image.pollinations.ai/prompt/{q}?width=1024&height=1024&nologo=true{s}"


def _download_images_from_urls(urls: list[str], *, limit: int) -> list[bytes]:
    blobs: list[bytes] = []
    for u in urls:
        if len(blobs) >= limit:
            break
        if not str(u).strip():
            continue
        try:
            blobs.append(_download_image_bytes(str(u).strip(), timeout_sec=120))
        except Exception as exc:  # noqa: BLE001
            logger.warning("Không tải được ảnh URL {}: {}", u, exc)
    return blobs


def _download_images_from_urls_via_browser(page, urls: list[str], *, limit: int) -> list[bytes]:
    """
    Tải ảnh qua Playwright browser context hiện tại (giữ cookie/session đã login).
    Rất hữu ích với URL ảnh yêu cầu xác thực của Gemini.
    """
    blobs: list[bytes] = []
    for u in urls:
        if len(blobs) >= limit:
            break
        su = str(u).strip()
        if not su:
            continue
        try:
            resp = page.context.request.get(su, timeout=120_000)
            if not resp.ok:
                continue
            data = resp.body()
            if data:
                blobs.append(bytes(data))
        except Exception as exc:  # noqa: BLE001
            logger.debug("Browser-download lỗi {}: {}", su, exc)
            continue
    return blobs


def _download_images_via_ui_download_button(page, *, limit: int) -> list[bytes]:
    """
    Tải ảnh bằng nút Download xuất hiện khi hover ảnh trong Gemini UI.
    Dùng expect_download để lấy file thật từ browser session.
    Tránh trùng: bỏ qua file có cùng hash với ảnh đã tải (nút Download ``.first`` dễ lặp cùng một file).
    """
    blobs: list[bytes] = []
    seen_hashes: set[str] = set()
    try:
        imgs = page.locator("img")
        total = imgs.count()
    except Exception:
        total = 0
    if total <= 0:
        return blobs

    download_btn_selectors = [
        "button:has(mat-icon[fonticon='download'])",
        "[role='button']:has(mat-icon[fonticon='download'])",
        "button[aria-label*='download' i]",
        "[role='button'][aria-label*='download' i]",
    ]

    # Quét tối đa vài ảnh đầu đủ lớn, hover để hiện action bar rồi bấm Download.
    for i in range(min(total, 18)):
        if len(blobs) >= limit:
            break
        try:
            im = imgs.nth(i)
            if not im.is_visible():
                continue
            box = im.bounding_box()
            if not box:
                continue
            if box.get("width", 0) < 220 or box.get("height", 0) < 220:
                continue
            im.hover(timeout=3000)
            _human_pause(0.1, 0.25)
        except Exception:
            continue

        clicked_and_downloaded = False
        for sel in download_btn_selectors:
            if len(blobs) >= limit:
                break
            try:
                btn = page.locator(sel).first
                if not btn.is_visible():
                    continue
                with page.expect_download(timeout=7000) as dlinfo:
                    btn.click(timeout=3000)
                dl = dlinfo.value
                try:
                    p = dl.path()
                    if p:
                        data = Path(p).read_bytes()
                    else:
                        # fallback khi path không khả dụng (một số môi trường).
                        data = dl.content()  # type: ignore[attr-defined]
                    if data:
                        h = hashlib.sha256(bytes(data)).hexdigest()[:32]
                        if h in seen_hashes:
                            clicked_and_downloaded = True
                            break
                        seen_hashes.add(h)
                        blobs.append(bytes(data))
                        clicked_and_downloaded = True
                        break
                except Exception as exc:  # noqa: BLE001
                    logger.debug("Đọc file download lỗi: {}", exc)
                    continue
            except Exception:
                continue
        if clicked_and_downloaded:
            _human_pause(0.15, 0.35)

    return blobs


def _has_visible_download_button(page) -> bool:
    selectors = [
        "button:has(mat-icon[fonticon='download'])",
        "[role='button']:has(mat-icon[fonticon='download'])",
        "button[aria-label*='download' i]",
        "[role='button'][aria-label*='download' i]",
    ]
    for sel in selectors:
        try:
            if page.locator(sel).first.is_visible():
                return True
        except Exception:
            continue
    return False


def _nanobanana_skip_gemini_tools_setup() -> bool:
    """Bật ``NANOBANANA_SKIP_GEMINI_TOOLS_SETUP=1`` để không chạy Tools → Create image → Pro."""
    return os.environ.get("NANOBANANA_SKIP_GEMINI_TOOLS_SETUP", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _nanobanana_gemini_tools_create_image_and_pro(page, *, prompt_selector: str) -> None:
    """
    Trên ``gemini.google.com``: Tools → **Create image** → mở **logo pill** model
    (``data-test-id=logo-pill-label-container``) → chọn dòng **Pro**
    (``span.mode-title`` + ``span.mode-desc`` "3.1 Pro").

    Dùng class/text theo UI Angular hiện tại; nếu không khớp (đã bật sẵn / A-B test) thì bỏ qua,
    không ném lỗi để không chặn luồng browser cũ.
    """
    if _nanobanana_skip_gemini_tools_setup():
        return
    try:
        host = (page.url or "").lower()
    except Exception:
        host = ""
    if "gemini.google.com" not in host:
        return

    # --- 1) Nút Tools (``toolbox-drawer-button-label-icon-text`` + span "Tools") ---
    try:
        tools = page.locator("div.toolbox-drawer-button-label-icon-text").filter(
            has=page.locator("span", has_text=re.compile(r"^\s*Tools\s*$", re.I))
        )
        tw = tools.first
        if not tw.is_visible(timeout=2800):
            logger.debug("Gemini UI: không thấy nút Tools visible, bỏ qua setup.")
            return
        tw.click(timeout=4500)
        _human_pause(0.35, 0.75)
    except Exception as exc:
        logger.debug("Gemini UI: không mở được Tools: {}", exc)
        return

    # --- 2) Mục Create image (``feature-content`` + ``label`` Create image) ---
    try:
        row = page.locator("div.feature-content").filter(
            has=page.locator("div.label", has_text=re.compile(r"Create\s*image", re.I))
        ).first
        row.wait_for(state="visible", timeout=5000)
        row.click(timeout=4500)
        _human_pause(0.5, 1.0)
    except Exception as exc:
        logger.debug("Gemini UI: không chọn Create image (có thể đã bật): {}", exc)
    finally:
        try:
            page.keyboard.press("Escape")
            _human_pause(0.15, 0.35)
        except Exception:
            pass

    # --- 3) Mở menu model (logo pill ``data-test-id=logo-pill-label-container``) → chọn **Pro** ---
    try:
        opened = False
        # Pill cạnh ô nhập: span "Pro/Fast/Thinking" + icon keyboard_arrow_down (DOM user cung cấp).
        for sel in (
            '[data-test-id="logo-pill-label-container"]',
            "div.logo-pill-label-container.input-area-switch-label:has(mat-icon[fonticon='keyboard_arrow_down'])",
        ):
            try:
                pl = page.locator(sel).first
                if pl.is_visible(timeout=2200):
                    pl.click(timeout=4500)
                    opened = True
                    break
            except Exception:
                continue

        if not opened:
            tier_btns = page.get_by_role("button", name=re.compile(r"^(Fast|Thinking|Pro)$", re.I))
            best = None
            best_y = -1.0
            n = min(tier_btns.count(), 20)
            for i in range(n):
                b = tier_btns.nth(i)
                try:
                    if not b.is_visible(timeout=800):
                        continue
                except Exception:
                    continue
                box = b.bounding_box()
                if not box:
                    continue
                y = float(box.get("y", 0.0))
                if y < 140:
                    continue
                if y > best_y:
                    best_y = y
                    best = b
            if best is None:
                anchor = page.locator(prompt_selector).first
                try:
                    shell = anchor.locator(
                        "xpath=ancestor::*[.//div[contains(@class,'toolbox-drawer-button-label-icon-text')]][1]"
                    )
                    tb = shell.locator("button").filter(
                        has_text=re.compile(r"^(Fast|Thinking|Pro)$", re.I)
                    ).first
                    if tb.is_visible(timeout=2000):
                        best = tb
                except Exception:
                    best = None
            if best is None:
                logger.debug("Gemini UI: không mở được menu model (logo pill / nút tier).")
                return
            best.click(timeout=4000)

        _human_pause(0.35, 0.7)

        # Dòng Pro trong overlay: ``span.mode-title`` + ``span.mode-desc`` (chính xác theo UI Gemini).
        pro_row = page.locator("div").filter(
            has=page.locator("span.mode-title.gds-label-l", has_text=re.compile(r"^\s*Pro\s*$", re.I))
        ).filter(
            has=page.locator(
                "span.mode-desc.gds-body-s",
                has_text=re.compile(r"Advanced\s+math\s+and\s+code\s+with\s+3\.1\s+Pro", re.I),
            )
        ).first
        if pro_row.is_visible(timeout=2800):
            pro_row.click(timeout=4500)
        else:
            sub = page.locator("div,button,[role='menuitem'],mat-list-item").filter(
                has_text=re.compile(r"Advanced\s+math\s+and\s+code\s+with\s+3\.1\s+Pro", re.I)
            )
            if sub.first.is_visible(timeout=1800):
                sub.first.click(timeout=4500)
            else:
                picked = False
                for role in ("menuitemradio", "menuitem", "option"):
                    try:
                        loc = page.get_by_role(role, name=re.compile(r"^\s*Pro\s*$", re.I))
                        if loc.first.is_visible(timeout=1200):
                            loc.first.click(timeout=4500)
                            picked = True
                            break
                    except Exception:
                        continue
                if not picked:
                    page.locator("div.labels div.label").filter(
                        has_text=re.compile(r"^\s*Pro\s*$", re.I)
                    ).first.click(timeout=4500)

        _human_pause(0.35, 0.75)
        logger.info("Gemini UI: đã chạy setup Tools → Create image → Pro (logo pill + mode row).")
    except Exception as exc:
        logger.debug("Gemini UI: bước model Pro không hoàn tất: {}", exc)
    finally:
        try:
            page.keyboard.press("Escape")
            _human_pause(0.12, 0.28)
        except Exception:
            pass


def _extract_image_urls_from_page(page) -> list[str]:
    script = """
(() => {
  const urls = [];
  const imgs = Array.from(document.querySelectorAll('img'));
  for (const img of imgs) {
    const src = (img.currentSrc || img.src || '').trim();
    if (!src) continue;
    if (src.startsWith('data:')) continue;
    if (/logo|avatar|icon|emoji|thumb/i.test(src)) continue;
    const w = img.naturalWidth || img.width || 0;
    const h = img.naturalHeight || img.height || 0;
    if (w >= 256 && h >= 256) urls.push(src);
  }
  return Array.from(new Set(urls));
})()
"""
    out = page.evaluate(script)
    if isinstance(out, list):
        return [str(x).strip() for x in out if str(x).strip()]
    return []


def _set_prompt_text_once(page, selector: str, prompt_text: str) -> None:
    """
    Set prompt theo kiểu idempotent: ghi đè 1 lần và verify, tránh append trùng.
    Hỗ trợ cả textarea/input và contenteditable.
    """
    txt = str(prompt_text or "").strip()
    if not txt:
        raise RuntimeError("Prompt rỗng.")
    loc = page.locator(selector).first
    # Ưu tiên fill cho input/textarea.
    try:
        loc.fill("", timeout=2500)
        _human_pause(0.1, 0.25)
        loc.fill(txt, timeout=4500)
        _human_pause(0.12, 0.3)
        val = str(
            loc.evaluate(
                """(el) => {
                  if (el instanceof HTMLTextAreaElement || el instanceof HTMLInputElement) return el.value || '';
                  return (el.textContent || '').trim();
                }"""
            )
            or ""
        )
        if val.strip() == txt:
            return
    except Exception:
        pass

    # Fallback cho contenteditable / UI custom: set trực tiếp DOM + dispatch input.
    try:
        loc.click(timeout=2500)
        _human_pause(0.1, 0.2)
        loc.evaluate(
            """(el, text) => {
              const t = String(text || '');
              if (el instanceof HTMLTextAreaElement || el instanceof HTMLInputElement) {
                el.value = t;
                el.dispatchEvent(new Event('input', { bubbles: true }));
                el.dispatchEvent(new Event('change', { bubbles: true }));
                return;
              }
              if (el instanceof HTMLElement) {
                if (el.getAttribute('contenteditable') === 'true' || el.isContentEditable) {
                  el.textContent = t;
                  el.dispatchEvent(new InputEvent('input', { bubbles: true, data: t, inputType: 'insertText' }));
                } else {
                  el.textContent = t;
                  el.dispatchEvent(new Event('input', { bubbles: true }));
                }
              }
            }""",
            txt,
        )
        _human_pause(0.12, 0.3)
        val2 = str(
            loc.evaluate(
                """(el) => {
                  if (el instanceof HTMLTextAreaElement || el instanceof HTMLInputElement) return el.value || '';
                  return (el.textContent || '').trim();
                }"""
            )
            or ""
        )
        if val2.strip() == txt:
            return
    except Exception as exc:
        raise RuntimeError(f"Không set được prompt bằng DOM: {exc}") from exc

    raise RuntimeError("Set prompt không khớp sau khi verify.")


def _click_send_or_submit(page, *, chosen_prompt: str, gen_selectors: list[str]) -> bool:
    """
    Click nút Send/Submit thật chắc chắn. Fallback Enter/Ctrl+Enter.
    """
    _ = gen_selectors  # giữ tương thích interface, hiện không dùng click selector để tránh lệch nút.

    # Ưu tiên click đúng nút send cứng theo DOM Gemini user cung cấp.
    hard_button_selectors = [
        "button.send-button.submit[aria-label='Send message'][aria-disabled='false']",
        "button.send-button[aria-label='Send message'][aria-disabled='false']",
        "button.send-button.submit[aria-label='Send message']",
        "button.send-button[aria-label='Send message']",
    ]
    for sel in hard_button_selectors:
        try:
            btn = page.locator(sel).first
            if not btn.is_visible():
                continue
            try:
                disabled = bool(
                    btn.evaluate(
                        "(el) => el.getAttribute('aria-disabled') === 'true' || (el instanceof HTMLButtonElement && !!el.disabled)"
                    )
                )
                if disabled:
                    continue
            except Exception:
                pass
            _human_pause(0.1, 0.2)
            btn.click(timeout=3500)
            _human_pause(0.2, 0.4)
            return True
        except Exception:
            continue

    # Ưu tiên gửi bằng phím để tránh click nhầm vào nút Thinking.
    try:
        page.locator(chosen_prompt).first.click(timeout=2200)
        _human_pause(0.12, 0.28)
        page.keyboard.press("Control+Enter")
        _human_pause(0.25, 0.45)
        return True
    except Exception:
        pass
    try:
        page.locator(chosen_prompt).first.click(timeout=2200)
        _human_pause(0.12, 0.28)
        page.keyboard.press("Enter")
        _human_pause(0.25, 0.45)
        return True
    except Exception:
        pass

    # Fallback cho Gemini UI kiểu Angular Material:
    # <mat-icon ... class="send-button-icon" fonticon="send">send</mat-icon>
    icon_selectors = [
        "mat-icon[fonticon='send']",
        "mat-icon.send-button-icon",
        "mat-icon.google-symbols:has-text('send')",
    ]
    for sel in icon_selectors:
        try:
            icon = page.locator(sel).first
            if not icon.is_visible():
                continue
            # Bấm phần tử cha clickable gần nhất thay vì bấm thẳng icon nếu có.
            clicked = bool(
                icon.evaluate(
                    """(el) => {
                      const isDisabled = (n) =>
                        !!(n && (
                          n.getAttribute?.('aria-disabled') === 'true' ||
                          n.getAttribute?.('disabled') !== null ||
                          n.classList?.contains('disabled')
                        ));
                      const clickables = [
                        el.closest('button'),
                        el.closest('[role="button"]'),
                        el.closest('.send-button'),
                        el.closest('.send-button-container'),
                        el.parentElement,
                      ];
                      for (const n of clickables) {
                        if (!n || isDisabled(n)) continue;
                        if (n instanceof HTMLElement) {
                          n.click();
                          return true;
                        }
                      }
                      if (el instanceof HTMLElement && !isDisabled(el)) {
                        el.click();
                        return true;
                      }
                      return false;
                    }"""
                )
            )
            if clicked:
                _human_pause(0.2, 0.45)
                return True
        except Exception:
            continue

    # Không click tự động để tránh bấm lệch qua Thinking.
    return False


def _is_send_button_disabled(page) -> bool:
    """
    Kiểm tra nhanh nút gửi có đang disabled (thường là đang xử lý generation) hay không.
    """
    selectors = _locked_submit_selectors()
    for sel in selectors:
        try:
            btn = page.locator(sel).first
            if not btn.is_visible():
                continue
            disabled = bool(
                btn.evaluate(
                    "(el) => el.getAttribute('aria-disabled') === 'true' || (el instanceof HTMLButtonElement && !!el.disabled)"
                )
            )
            return disabled
        except Exception:
            continue
    return False


def _nanobanana_pick_best_new_image_url(page, baseline_urls: set[str]) -> str | None:
    """Chọn src ảnh lớn nhất trên trang mà chưa có trong ``baseline_urls`` (ảnh kết quả mới)."""
    try:
        data = page.evaluate(
            """(baseArr) => {
              const baseline = new Set(baseArr);
              let best = '', bestA = 0;
              for (const img of document.querySelectorAll('img')) {
                const src = (img.currentSrc || img.src || '').trim();
                if (!src || src.startsWith('data:')) continue;
                if (/logo|avatar|icon|emoji|thumb/i.test(src)) continue;
                if (baseline.has(src)) continue;
                const w = img.naturalWidth || img.width || 0;
                const h = img.naturalHeight || img.height || 0;
                if (w < 200 || h < 200) continue;
                const a = w * h;
                if (a > bestA) { bestA = a; best = src; }
              }
              return best || '';
            }""",
            list(baseline_urls),
        )
    except Exception:
        return None
    s = str(data or "").strip()
    return s or None


def _nanobanana_wait_for_new_result_image_url(
    page,
    *,
    baseline_urls: set[str],
    chosen_prompt: str,
    gen_selectors: list[str],
    wait_timeout_ms: int,
) -> str | None:
    """Chờ sau Send cho đến khi xuất hiện URL ảnh mới (ngoài baseline)."""
    deadline = time.time() + max(30, wait_timeout_ms / 1000.0)
    saw_busy = False
    send_retry_done = False
    idle_ticks = 0
    while time.time() < deadline:
        busy = _is_send_button_disabled(page)
        if busy:
            saw_busy = True
            _nb_status("GEN_BUSY", "UI đang xử lý ảnh...")
            idle_ticks = 0
        else:
            idle_ticks += 1
        cand = _nanobanana_pick_best_new_image_url(page, baseline_urls)
        if cand and (not saw_busy or not busy):
            return cand
        if _has_visible_download_button(page):
            cand = _nanobanana_pick_best_new_image_url(page, baseline_urls)
            if cand:
                return cand
        if (not saw_busy) and (idle_ticks >= 4) and (not send_retry_done):
            _human_pause(0.2, 0.4)
            _click_send_or_submit(page, chosen_prompt=chosen_prompt, gen_selectors=gen_selectors)
            _nb_status("SEND_RETRY", "Thử gửi lại 1 lần vì chưa thấy tín hiệu xử lý.")
            send_retry_done = True
        time.sleep(1.2)
    return None


def _nanobanana_download_one_result_blob(page, url: str | None) -> bytes | None:
    """Tải một ảnh: ưu tiên GET URL trong session; fallback nút Download (1 file)."""
    if url:
        try:
            resp = page.context.request.get(url, timeout=120_000)
            if resp.ok:
                body = resp.body()
                if body and len(body) > 2000:
                    return bytes(body)
        except Exception as exc:  # noqa: BLE001
            logger.debug("Browser GET ảnh lỗi: {}", exc)
    tmp = _download_images_via_ui_download_button(page, limit=1)
    return tmp[0] if tmp else None


def generate_post_images_nano_banana_browser(
    *,
    prompt: str,
    number_of_images: int = 1,
) -> list[bytes]:
    """
    Tạo ảnh bằng tự động hóa trình duyệt NanoBanana (browser riêng, lưu phiên đăng nhập).

    Luồng:
    - Mở persistent profile riêng ``data/nanobanana/browser_profile``
    - Điều hướng trang Gemini web
    - Tools → Create image → Pro (một lần)
    - Với mỗi ảnh cần lấy: nhập prompt (có biến thể khi ``number_of_images`` > 1), Send, chờ ảnh mới, tải **đúng một** file
      (tránh lặp 4 lần cùng một nút Download / cùng một URL).
    """
    n = max(1, min(4, int(number_of_images)))
    _nb_status("START", f"Khởi tạo browser flow, number_of_images={n}")
    web_url = (
        os.environ.get("NANOBANANA_WEB_URL", "https://gemini.google.com/app?hl=en").strip()
        or "https://gemini.google.com/app?hl=en"
    )
    prompt_selector = os.environ.get("NANOBANANA_PROMPT_SELECTOR", "").strip()
    gen_selector = os.environ.get("NANOBANANA_GENERATE_SELECTOR", "").strip()
    wait_timeout_ms = int(float(os.environ.get("NANOBANANA_WAIT_MS", "120000")))
    manual_login_sec = int(float(os.environ.get("NANOBANANA_MANUAL_LOGIN_SEC", "35")))
    profile_dir = _nanobanana_browser_profile_dir()
    with sync_playwright() as pw:
        context = _launch_nb_context(pw, profile_dir=profile_dir)
        try:
            try:
                stealth = Stealth()
                stealth.apply_stealth_sync(context)
            except Exception:
                pass

            page = _open_fresh_page(context, target_url=web_url)
            page.set_default_timeout(10_000)
            _nb_status("OPEN_WEB", f"Đã mở {web_url}")
            _human_pause(0.8, 1.4)

            prompt_selectors = [prompt_selector] if prompt_selector else []
            if _nanobanana_locked_ui_mode():
                prompt_selectors.extend(_locked_prompt_selectors())
            else:
                prompt_selectors.extend(
                    [
                        "textarea",
                        "textarea[placeholder*='prompt' i]",
                        "textarea[placeholder*='describe' i]",
                        "[contenteditable='true']",
                        "[role='textbox']",
                        "input[type='text'][placeholder*='prompt' i]",
                    ]
                )
            gen_selectors = (
                [x.strip() for x in gen_selector.split(",") if x.strip()]
                if gen_selector
                else [
                    *_locked_submit_selectors(),
                ]
            )

            chosen_prompt = ""
            try:
                for ps in prompt_selectors:
                    try:
                        page.wait_for_selector(ps, timeout=6_000)
                        chosen_prompt = ps
                        break
                    except Exception:
                        continue
                if not chosen_prompt:
                    raise RuntimeError("Chưa thấy ô prompt")
            except Exception:
                logger.info(
                    "NanoBanana browser: chưa thấy prompt box, chờ login tay {}s (profile={}).",
                    manual_login_sec,
                    profile_dir,
                )
                time.sleep(max(5, manual_login_sec))
                page.goto(web_url, wait_until="domcontentloaded", timeout=60_000)
                for ps in prompt_selectors:
                    try:
                        page.wait_for_selector(ps, timeout=8_000)
                        chosen_prompt = ps
                        break
                    except Exception:
                        continue
                if not chosen_prompt:
                    _nanobanana_debug_screenshot(page, "prompt_not_found")
                    raise RuntimeError("Không tìm thấy ô prompt trên giao diện NanoBanana sau khi chờ login.")
            _nb_status("PROMPT_BOX_OK", f"Đã tìm thấy ô prompt selector={chosen_prompt}")

            stabilize_ms = int(float(os.environ.get("NANOBANANA_STABILIZE_MS", "2500")))
            tools_once = False
            out_blobs: list[bytes] = []
            seen_digests: set[str] = set()

            for round_i in range(n):
                base = (prompt or "").strip()
                if len(base) > 1100:
                    base = base[:1100] + "\n[...]"
                if n > 1:
                    suffix = (
                        f"\n\n[Portrait variant {round_i + 1} of {n}]\n"
                        "Generate exactly ONE image. Change head tilt, gaze, shoulder line, or lighting "
                        "while keeping the SAME character identity, age, wardrobe, skin tone, and rendering "
                        "style as in the main description. No collage, no split panel, no multiple faces."
                    )
                else:
                    suffix = ""
                full_prompt = (base + suffix)[:1500]

                if not tools_once:
                    _nanobanana_gemini_tools_create_image_and_pro(page, prompt_selector=chosen_prompt)
                    tools_once = True

                baseline_now = set(_extract_image_urls_from_page(page))
                try:
                    _set_prompt_text_once(page, chosen_prompt, full_prompt)
                    _human_pause(0.25, 0.6)
                except Exception as exc:  # noqa: BLE001
                    _nanobanana_debug_screenshot(page, "prompt_fill_failed")
                    raise RuntimeError(
                        f"Điền prompt thất bại (vòng {round_i + 1}/{n}) selector={chosen_prompt!r}: {exc}"
                    ) from exc
                _nb_status(
                    "PROMPT_SET_OK",
                    f"Vòng {round_i + 1}/{n}: đã nhập prompt ({len(full_prompt)} ký tự)",
                )

                _human_pause(0.35, 0.75)
                clicked = _click_send_or_submit(page, chosen_prompt=chosen_prompt, gen_selectors=gen_selectors)
                if not clicked:
                    _nanobanana_debug_screenshot(page, "generate_button_not_found")
                    raise RuntimeError("Không tìm thấy/bấm được nút Submit/Send trên giao diện Gemini.")
                _nb_status("SEND_OK", f"Vòng {round_i + 1}/{n}: đã gửi, chờ ảnh mới…")

                new_url = _nanobanana_wait_for_new_result_image_url(
                    page,
                    baseline_urls=baseline_now,
                    chosen_prompt=chosen_prompt,
                    gen_selectors=gen_selectors,
                    wait_timeout_ms=wait_timeout_ms,
                )
                if stabilize_ms > 0:
                    time.sleep(stabilize_ms / 1000.0)

                blob = _nanobanana_download_one_result_blob(page, new_url)
                if not blob:
                    _nanobanana_debug_screenshot(page, f"no_result_image_r{round_i + 1}")
                    raise RuntimeError(
                        f"Vòng {round_i + 1}/{n}: không tải được ảnh (không URL mới hoặc download lỗi)."
                    )

                digest = hashlib.sha256(blob).hexdigest()[:32]
                if digest in seen_digests:
                    logger.warning(
                        "NanoBanana browser: vòng {}/{} ảnh trùng hash với vòng trước — vẫn giữ để đủ số lượng.",
                        round_i + 1,
                        n,
                    )
                seen_digests.add(digest)
                out_blobs.append(blob)
                logger.info("NanoBanana browser: vòng {}/{} OK ({} bytes).", round_i + 1, n, len(blob))

                if round_i < n - 1:
                    time.sleep(_nano_banana_pro_inter_image_delay_sec())

            _nb_status("DOWNLOAD_OK", f"Tải thành công {len(out_blobs)} ảnh (tuần tự).")
            logger.info("NanoBanana browser OK ({} ảnh, {} vòng).", len(out_blobs), n)
            return out_blobs
        finally:
            try:
                context.close()
            except Exception:
                pass


def _extract_image_blobs_from_payload(payload: dict[str, object]) -> list[bytes]:
    blobs: list[bytes] = []
    candidates = payload.get("data")
    if not isinstance(candidates, list):
        candidates = payload.get("images")
    if not isinstance(candidates, list):
        candidates = []
    for item in candidates:
        if not isinstance(item, dict):
            continue
        b64 = str(item.get("b64_json", "") or item.get("base64", "")).strip()
        if b64:
            try:
                blobs.append(base64.b64decode(b64))
                continue
            except Exception:  # noqa: BLE001
                pass
        url = str(item.get("url", "") or item.get("image_url", "")).strip()
        if url:
            try:
                blobs.append(_download_image_bytes(url))
            except Exception as exc:  # noqa: BLE001
                logger.warning("Không tải được ảnh NanoBanana URL {}: {}", url, exc)
    return blobs


def _extract_result_image_urls(payload: dict[str, object]) -> list[str]:
    urls: list[str] = []
    # common shape: {"response": {"resultImageUrl": "..."}}
    resp = payload.get("response")
    if isinstance(resp, dict):
        one = str(resp.get("resultImageUrl", "")).strip()
        if one:
            urls.append(one)
        arr = resp.get("resultImageUrls")
        if isinstance(arr, list):
            urls.extend([str(x).strip() for x in arr if str(x).strip()])
    # fallback shapes
    for k in ("resultImageUrl", "imageUrl", "url"):
        one = str(payload.get(k, "")).strip()
        if one:
            urls.append(one)
    arr2 = payload.get("resultImageUrls") or payload.get("imageUrls")
    if isinstance(arr2, list):
        urls.extend([str(x).strip() for x in arr2 if str(x).strip()])
    # unique
    out: list[str] = []
    seen: set[str] = set()
    for u in urls:
        if u not in seen:
            out.append(u)
            seen.add(u)
    return out


def _poll_nanobanana_task_result(
    task_id: str,
    *,
    api_key: str,
    timeout_sec: int = 150,
    interval_sec: float = 2.5,
) -> list[bytes]:
    import time

    url = _nanobanana_record_info_url(task_id)
    headers = {"Authorization": f"Bearer {api_key}"}
    started = time.time()
    last_payload: dict[str, object] | None = None
    while (time.time() - started) < timeout_sec:
        resp = requests.get(url, headers=headers, timeout=60)
        resp.raise_for_status()
        payload = resp.json()
        if isinstance(payload, dict):
            last_payload = payload
            sf = payload.get("successFlag")
            try:
                success_flag = int(sf) if sf is not None else -1
            except (TypeError, ValueError):
                success_flag = -1
            if success_flag == 1:
                blobs = _extract_image_blobs_from_payload(payload)
                if blobs:
                    return blobs
                urls = _extract_result_image_urls(payload)
                if urls:
                    return [_download_image_bytes(u, timeout_sec=120) for u in urls]
                raise RuntimeError("Task SUCCESS nhưng không có resultImageUrl.")
            if success_flag in (2, 3):
                raise RuntimeError(f"Task thất bại successFlag={success_flag}: {payload}")
        time.sleep(interval_sec)
    raise RuntimeError(f"NanoBanana poll timeout task_id={task_id}, last={last_payload}")


def generate_post_images_nano_banana(
    *,
    prompt: str,
    number_of_images: int = 1,
) -> list[bytes]:
    """
    Sinh ảnh qua NanoBanana API.

    Yêu cầu env:
    - ``NANOBANANA_API_KEY``
    - (tuỳ chọn) ``NANOBANANA_API_URL``, ``NANOBANANA_MODEL``.
    """
    if _nanobanana_browser_enabled():
        try:
            _img_status("PROVIDER_NANOBANANA", "Ưu tiên browser automation.")
            return generate_post_images_nano_banana_browser(
                prompt=prompt,
                number_of_images=number_of_images,
            )
        except Exception as exc:  # noqa: BLE001
            if _nanobanana_browser_strict():
                raise RuntimeError(
                    "Sinh ảnh browser thất bại. Đang ở chế độ bắt buộc dùng browser profile đã đăng nhập, "
                    "không fallback API. Vui lòng đăng nhập lại bằng nút 'Đăng nhập Gemini Image' và thử lại."
                ) from exc
            logger.warning("NanoBanana browser lỗi, fallback API: {}", exc)

    keys = _nanobanana_api_keys()
    if not keys:
        raise RuntimeError("Thiếu NANOBANANA_API_KEY (hoặc NANOBANANA_API_KEYS / VEO3_API_KEY) để sinh ảnh NanoBanana.")
    n = max(1, min(4, int(number_of_images)))
    url = _nanobanana_url()
    body = {
        "prompt": (prompt or "")[:4800],
        "type": "TEXTTOIAMGE",
        "numImages": n,
        "image_size": "1:1",
        "callBackUrl": _nanobanana_callback_url(),
    }
    model_name = _nanobanana_model()
    if model_name:
        body["model"] = model_name
    # Shuffle để phân tải giữa nhiều account/key (gen nhanh + đỡ rate-limit cục bộ).
    order = list(keys)
    random.shuffle(order)
    last_exc: Exception | None = None
    for api_key in order:
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        try:
            _img_status("NB_API_SUBMIT", "Đang submit task NanoBanana API...")
            resp = requests.post(url, json=body, headers=headers, timeout=120)
            resp.raise_for_status()
            payload = resp.json()
            if not isinstance(payload, dict):
                raise RuntimeError("NanoBanana JSON không hợp lệ.")
            # API task-based: code=200 + data.taskId, sau đó poll record-info.
            task_id = ""
            data = payload.get("data")
            if isinstance(data, dict):
                task_id = str(data.get("taskId", "")).strip()
            if task_id:
                _img_status("NB_API_POLL", f"Đang poll task_id={task_id}")
                blobs = _poll_nanobanana_task_result(task_id, api_key=api_key)
                logger.info("NanoBanana OK task={} ({} ảnh) với pool {} key.", task_id, len(blobs), len(order))
                return blobs
            # fallback hỗ trợ nếu nhà cung cấp trả ảnh trực tiếp
            blobs = _extract_image_blobs_from_payload(payload)
            if blobs:
                logger.info("NanoBanana direct OK ({} ảnh) với pool {} key.", len(blobs), len(order))
                return blobs
            urls = _extract_result_image_urls(payload)
            if urls:
                blobs = [_download_image_bytes(u, timeout_sec=120) for u in urls]
                logger.info("NanoBanana direct URL OK ({} ảnh) với pool {} key.", len(blobs), len(order))
                return blobs
            raise RuntimeError(f"NanoBanana không trả taskId/ảnh hợp lệ: {payload}")
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            logger.warning("NanoBanana lỗi với một key trong pool: {}", exc)
            continue
    raise RuntimeError(f"NanoBanana lỗi trên toàn bộ key pool ({len(order)} key): {last_exc!r}") from last_exc


def generate_post_images_pollinations(
    *,
    prompt: str,
    number_of_images: int = 1,
) -> list[bytes]:
    """
    Fallback công khai: Pollinations (không cần key).
    Dùng khi NanoBanana/Imagen không khả dụng để vẫn tạo ảnh tự động.
    """
    n = max(1, min(4, int(number_of_images)))
    blobs: list[bytes] = []
    for i in range(n):
        url = _pollinations_url(prompt, seed=1000 + i)
        try:
            blobs.append(_download_image_bytes(url, timeout_sec=120))
        except Exception as exc:  # noqa: BLE001
            logger.warning("Pollinations lỗi ảnh {}: {}", i + 1, exc)
    if not blobs:
        raise RuntimeError("Pollinations không trả về ảnh hợp lệ.")
    logger.info("Pollinations OK ({} ảnh).", len(blobs))
    return blobs


def generate_post_images_png(
    *,
    prompt: str,
    number_of_images: int = 1,
    api_key: str | None = None,
    model: str | None = None,
    provider: Literal["auto", "nanobanana", "nano_banana_pro", "imagen", "pollinations"] = "auto",
) -> list[bytes]:
    """
    Gọi Imagen, trả về danh sách bytes PNG (mỗi phần tử một ảnh).

    Raises:
        RuntimeError: thiếu key, thiếu gói, hoặc API lỗi.
    """
    pv = str(provider or "auto").strip().lower()
    _img_status("START", f"provider={pv}, number_of_images={number_of_images}")
    if pv == "pollinations":
        _img_status("PROVIDER_POLLINATIONS", "Chạy Pollinations trực tiếp.")
        return generate_post_images_pollinations(prompt=prompt, number_of_images=number_of_images)

    if pv == "nanobanana":
        _img_status("PROVIDER_NANOBANANA", "Chạy NanoBanana trực tiếp.")
        return generate_post_images_nano_banana(prompt=prompt, number_of_images=number_of_images)

    if pv in {"nano_banana_pro", "nanobanana_pro"}:
        _img_status("PROVIDER_NANO_BANANA_PRO", "Chạy Nano Banana Pro (Gemini API).")
        from src.services.ai_image_config import nano_banana_pro_settings

        nb_cfg = nano_banana_pro_settings()
        max_retries = int(nb_cfg.get("max_retries") or 3)
        key_env = str(nb_cfg.get("api_key_env") or "GEMINI_API_KEY").strip() or "GEMINI_API_KEY"
        resolved_key = (api_key or os.environ.get(key_env, "")).strip()
        return generate_post_images_nano_banana_pro(
            prompt=prompt,
            number_of_images=number_of_images,
            api_key=resolved_key or None,
            model=model,
            max_retries=max_retries,
        )

    # auto: ưu tiên Imagen (Gemini API) trước, NanoBanana là fallback.
    api_key = (api_key or os.environ.get("GEMINI_API_KEY", "")).strip()
    last_exc: Exception | None = None
    if api_key:
        try:
            from google import genai
            from google.genai import types
        except ImportError as exc:
            if pv == "imagen":
                raise RuntimeError("Cần cài google-genai: pip install google-genai") from exc
            last_exc = exc
        else:
            n = max(1, min(4, int(number_of_images)))
            client = genai.Client(api_key=api_key)
            for mname in _imagen_model_candidates(model):
                try:
                    resp = client.models.generate_images(
                        model=mname,
                        prompt=(prompt or "")[:4800],
                        config=types.GenerateImagesConfig(
                            number_of_images=n,
                            aspect_ratio="1:1",
                        ),
                    )
                    blobs: list[bytes] = []
                    for item in getattr(resp, "generated_images", None) or []:
                        img = getattr(item, "image", None)
                        data = getattr(img, "image_bytes", None) if img is not None else None
                        if data:
                            blobs.append(bytes(data))
                    if blobs:
                        logger.info("Imagen OK model={} ({} ảnh).", mname, len(blobs))
                        _img_status("IMAGEN_OK", f"model={mname}, images={len(blobs)}")
                        return blobs
                    raise RuntimeError("Imagen trả về danh sách ảnh rỗng.")
                except Exception as exc:  # noqa: BLE001
                    last_exc = exc
                    logger.warning("Imagen model {}: {}", mname, exc)
                    continue
    elif pv == "imagen":
        raise RuntimeError("Thiếu GEMINI_API_KEY để sinh ảnh (Imagen).")

    # auto fallback: thử NanoBanana sau Imagen
    if pv == "auto" and os.environ.get("NANOBANANA_API_KEY", "").strip():
        try:
            _img_status("AUTO_FALLBACK_NB", "Imagen lỗi/không khả dụng, fallback NanoBanana.")
            return generate_post_images_nano_banana(
                prompt=prompt,
                number_of_images=number_of_images,
            )
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            logger.warning("Auto fallback NanoBanana lỗi: {}", exc)

    hint = ""
    raw = str(last_exc or "")
    if "NOT_FOUND" in raw or "not found for API version" in raw.lower():
        hint = (
            " API key hiện tại chưa truy cập được Imagen model trên Gemini API "
            "(thường do chưa bật quyền/billing hoặc model không khả dụng với key)."
        )
    # Nếu ép provider=imagen thì không fallback sang nguồn khác.
    if pv == "imagen":
        raise RuntimeError(
            f"Imagen lỗi (provider=imagen). Lỗi cuối: {last_exc!r}.{hint}"
        ) from last_exc

    # auto: fallback cuối Pollinations (không cần key), để vẫn auto-sinh ảnh cho batch.
    try:
        return generate_post_images_pollinations(prompt=prompt, number_of_images=number_of_images)
    except Exception as p_exc:  # noqa: BLE001
        raise RuntimeError(
            f"Không sinh được ảnh (đã thử NanoBanana/Imagen/Pollinations). "
            f"Lỗi Imagen cuối: {last_exc!r}.{hint} | Lỗi Pollinations: {p_exc!r}"
        ) from p_exc


def generate_and_save_images_for_page_batch(
    page_id: str,
    *,
    file_stem: str,
    title: str,
    body: str,
    image_style: str = "",
    number_of_images: int = 1,
    provider: Literal["auto", "nanobanana", "nano_banana_pro", "imagen", "pollinations"] = "auto",
    image_prompt: str = "",
) -> list[Path]:
    """
    Sinh ảnh theo câu chuyện bài viết, lưu dưới ``library/generated`` của Page.

    Returns:
        Đường dẫn tuyệt đối tới từng file ``.png``.
    """
    prompt = str(image_prompt).strip() or build_imagen_prompt_from_post(title=title, body=body, image_style=image_style)
    _img_status("PROMPT_READY", f"page_id={page_id}, prompt_len={len(prompt)}")
    blobs = generate_post_images_png(
        prompt=prompt,
        number_of_images=number_of_images,
        provider=provider,
    )
    safe = re.sub(r"[^a-zA-Z0-9._-]+", "_", file_stem).strip("._")[:72] or uuid.uuid4().hex[:12]
    root = ensure_page_workspace(page_id)
    out_dir = root / "library" / "generated"
    out_dir.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []
    for j, blob in enumerate(blobs):
        name = f"{safe}.png" if len(blobs) == 1 else f"{safe}_{j}.png"
        path = out_dir / name
        path.write_bytes(blob)
        paths.append(path.resolve())
        logger.info("Đã lưu ảnh AI: {}", path)
        _img_status("SAVE_FILE", f"{path}")
    return paths
