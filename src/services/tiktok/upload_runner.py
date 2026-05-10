from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from loguru import logger
from playwright.sync_api import BrowserContext, Page, TimeoutError as PlaywrightTimeoutError, sync_playwright

from src.services.tiktok.layout import ensure_tiktok_layout

LogFn = Callable[[str], None]
JobPatchFn = Callable[[dict[str, Any]], None]

TIKTOK_UPLOAD_URLS = (
    "https://www.tiktok.com/upload",
    "https://www.tiktok.com/tiktokstudio/upload",
    "https://www.tiktok.com/creator-center/upload",
)


def _screenshot_path(job_id: str, step: str) -> Path:
    paths = ensure_tiktok_layout()
    safe_step = re.sub(r"[^\w\-]+", "_", (step or "error").strip())[:80]
    return paths["screenshots"] / f"{job_id}_{safe_step}.png"


def save_tiktok_screenshot(page: Page | None, job: dict[str, Any]) -> str:
    if page is None:
        return ""
    jid = str(job.get("id", "job"))
    step = str(job.get("step", "error"))
    p = _screenshot_path(jid, step)
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        page.screenshot(path=str(p), full_page=True)
        return str(p)
    except Exception as exc:  # noqa: BLE001
        logger.warning("TikTok screenshot thất bại: {}", exc)
        return ""


def _playwright_proxy(account: dict[str, Any]) -> dict[str, Any] | None:
    px = account.get("proxy")
    if not isinstance(px, dict):
        return None
    if not bool(px.get("enabled")):
        return None
    server = str(px.get("server", "")).strip()
    if not server:
        return None
    if not server.startswith(("http://", "https://", "socks5://")):
        server = f"http://{server}"
    out: dict[str, Any] = {"server": server}
    u = str(px.get("username", "")).strip()
    pw = str(px.get("password", "")).strip()
    if u:
        out["username"] = u
        out["password"] = pw
    return out


def open_tiktok_browser(account: dict[str, Any], *, playwright: Any) -> BrowserContext:
    profile = Path(str(account.get("profile_path", "")).strip())
    profile.mkdir(parents=True, exist_ok=True)
    exe_raw = str(account.get("browser_exe_path", "")).strip()
    exe = Path(exe_raw) if exe_raw else None

    launch_kwargs: dict[str, Any] = {
        "user_data_dir": str(profile.resolve()),
        "headless": False,
        "viewport": {"width": 1280, "height": 900},
        "accept_downloads": True,
    }
    if exe is not None and exe.is_file():
        launch_kwargs["executable_path"] = str(exe.resolve())

    proxy = _playwright_proxy(account)
    if proxy:
        launch_kwargs["proxy"] = proxy

    browser_type = str(account.get("browser_type", "chrome") or "chrome").strip().lower()
    if browser_type in ("firefox",):
        return playwright.firefox.launch_persistent_context(**launch_kwargs)
    return playwright.chromium.launch_persistent_context(**launch_kwargs)


def check_tiktok_login(page: Page) -> bool:
    try:
        url = (page.url or "").lower()
        if "login" in url and "tiktok.com" in url:
            return False
        body = page.locator("body").inner_text(timeout=10000).lower()
    except Exception:
        return False

    bad_signals = [
        "log in",
        "login",
        "sign up",
        "đăng nhập",
        "captcha",
        "verify your",
        "verify",
        "xác minh",
        "security check",
    ]
    if any(x in body for x in bad_signals):
        return False
    return True


def validate_tiktok_job(job: dict[str, Any], account: dict[str, Any]) -> None:
    if not str(account.get("id", "")).strip():
        raise ValueError("Thiếu account id.")
    if not str(account.get("profile_path", "")).strip():
        raise ValueError("Thiếu profile_path cho tài khoản TikTok.")
    vp = Path(str(job.get("video_path", "")).strip())
    if not vp.is_file():
        raise ValueError(f"Không tìm thấy file video: {vp}")


def build_tiktok_caption(job: dict[str, Any]) -> str:
    caption = str(job.get("caption", "")).strip()
    hashtags = job.get("hashtags", [])
    if isinstance(hashtags, list):
        hashtags_text = " ".join(str(x).strip() for x in hashtags if str(x).strip())
    else:
        hashtags_text = str(hashtags).strip()
    return " ".join(x for x in (caption, hashtags_text) if x)


def upload_tiktok_video(page: Page, video_path: str) -> None:
    path = Path(video_path)
    if not path.is_file():
        raise RuntimeError(f"File video không tồn tại: {path}")
    file_input = page.locator("input[type='file']").first
    file_input.wait_for(state="attached", timeout=30000)
    file_input.set_input_files(str(path.resolve()))


def wait_tiktok_video_ready(page: Page, timeout_ms: int = 300000) -> None:
    start = page.evaluate("Date.now()")

    busy_words = [
        "uploading",
        "processing",
        "đang tải",
        "đang xử lý",
        "preparing",
    ]

    while page.evaluate("Date.now()") - start < timeout_ms:
        try:
            body = page.locator("body").inner_text(timeout=5000).lower()
        except Exception:
            page.wait_for_timeout(3000)
            continue

        has_busy = any(w in body for w in busy_words)
        try:
            progress_count = page.locator("[role='progressbar']").count()
        except Exception:
            progress_count = 0

        if not has_busy and progress_count == 0:
            page.wait_for_timeout(2000)
            return

        page.wait_for_timeout(3000)

    raise RuntimeError("Hết thời gian chờ TikTok xử lý video sau khi tải lên.")


def input_tiktok_caption(page: Page, text: str) -> None:
    if not str(text).strip():
        return
    selectors = [
        "[contenteditable='true']",
        "div[contenteditable='true']",
        "textarea",
        "[role='textbox']",
    ]
    last_err: Exception | None = None
    for selector in selectors:
        try:
            loc = page.locator(selector).last
            if loc.count() == 0:
                continue
            loc.wait_for(state="visible", timeout=15000)
            loc.click()
            loc.press_sequentially(text, delay=20)
            return
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            continue
    raise RuntimeError("Không tìm thấy ô nhập caption TikTok.") from last_err


def apply_tiktok_settings(page: Page, job: dict[str, Any], log: LogFn) -> None:
    privacy = str(job.get("privacy", "public") or "public").strip().lower()
    if privacy not in ("public", "friends", "private"):
        log(f"[TikTok] Giá trị privacy không chuẩn ({privacy}) — giữ mặc định trên TikTok.")
    else:
        try:
            btn = page.get_by_role("combobox", name=re.compile("privacy|who can|ai có thể|view", re.I))
            if btn.count():
                btn.first.click()
                page.wait_for_timeout(600)
                label = {"public": "Public", "friends": "Friends", "private": "Private"}.get(privacy, "Public")
                opt = page.get_by_role("option", name=re.compile(re.escape(label), re.I))
                if opt.count():
                    opt.first.click()
                    page.wait_for_timeout(400)
        except Exception as exc:  # noqa: BLE001
            log(f"[TikTok] Không chỉnh privacy (bỏ qua): {exc}")

    toggles = [
        ("allow_comments", "comment", job.get("allow_comments", True)),
        ("allow_duet", "duet", job.get("allow_duet", True)),
        ("allow_stitch", "stitch", job.get("allow_stitch", True)),
    ]
    for _key, label_fragment, want in toggles:
        if not isinstance(want, bool):
            continue
        try:
            sw = page.get_by_role("switch", name=re.compile(label_fragment, re.I))
            if sw.count() == 0:
                continue
            el = sw.first
            try:
                pressed = el.get_attribute("aria-checked") == "true"
            except Exception:
                pressed = False
            if want != pressed:
                el.click()
                page.wait_for_timeout(300)
        except Exception as exc:  # noqa: BLE001
            log(f"[TikTok] Không chỉnh {label_fragment} (bỏ qua): {exc}")


def click_tiktok_post(page: Page) -> None:
    patterns = re.compile(r"Post|Đăng|Publish", re.I)
    btn = page.get_by_role("button", name=patterns).last
    btn.wait_for(state="visible", timeout=30000)
    dis = btn.get_attribute("disabled")
    if dis is not None and dis != "":
        raise RuntimeError("Nút Post TikTok đang disabled.")
    if btn.get_attribute("aria-disabled") == "true":
        raise RuntimeError("Nút Post TikTok đang aria-disabled.")
    btn.click()


def verify_tiktok_post_success(page: Page, timeout_ms: int = 120000) -> None:
    start = page.evaluate("Date.now()")
    success_words = [
        "posted",
        "uploaded",
        "your video is being uploaded",
        "video posted",
        "đã đăng",
        "tải lên thành công",
        "upload complete",
    ]

    while page.evaluate("Date.now()") - start < timeout_ms:
        try:
            body = page.locator("body").inner_text(timeout=5000).lower()
        except Exception:
            page.wait_for_timeout(3000)
            continue
        if any(w in body for w in success_words):
            return
        url = (page.url or "").lower()
        if "/upload" not in url and "tiktok.com" in url:
            return
        page.wait_for_timeout(3000)

    raise RuntimeError("Không xác minh được TikTok đã đăng thành công.")


def _goto_upload_page(page: Page, log: LogFn) -> None:
    last_err: Exception | None = None
    for url in TIKTOK_UPLOAD_URLS:
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(2000)
            return
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            log(f"[TikTok] Thử URL upload: {url} — lỗi: {exc}")
    raise RuntimeError(f"Không mở được trang upload TikTok: {last_err}")


def _mark(
    job: dict[str, Any],
    *,
    status: str,
    step: str,
    error: str,
    patch_job: JobPatchFn,
) -> None:
    job["status"] = status
    job["step"] = step
    job["error_message"] = error
    patch_job(dict(job))


def run_tiktok_upload_job_sync(
    job: dict[str, Any],
    account: dict[str, Any],
    *,
    log: LogFn,
    patch_job: JobPatchFn,
) -> None:
    validate_tiktok_job(job, account)

    context: BrowserContext | None = None
    page: Page | None = None

    def _fail_need_manual(msg: str, step: str) -> None:
        save_tiktok_screenshot(page, job)
        _mark(job, status="need_manual_check", step=step, error=msg, patch_job=patch_job)
        log(f"[TikTok] need_manual_check: {msg}")

    def _fail_retryable(msg: str, step: str) -> None:
        save_tiktok_screenshot(page, job)
        rc = int(job.get("retry_count") or 0)
        mx = int(job.get("max_retry") or 2)
        job["retry_count"] = rc + 1
        if job["retry_count"] > mx:
            _mark(job, status="failed", step=step, error=msg, patch_job=patch_job)
        else:
            _mark(job, status="pending", step=step, error=msg, patch_job=patch_job)
        log(f"[TikTok] Lỗi ({step}): {msg}")

    try:
        with sync_playwright() as p:
            log("[TikTok] Đang mở browser profile…")
            context = open_tiktok_browser(account, playwright=p)
            page = context.new_page()
            try:
                patch_job({"step": "OPEN_UPLOAD_PAGE", "status": "running"})
                _goto_upload_page(page, log)
                page.wait_for_timeout(2000)

                if not check_tiktok_login(page):
                    _fail_need_manual("TikTok chưa đăng nhập hoặc cần xác minh/captcha.", "CHECK_LOGIN")
                    return

                patch_job({"step": "UPLOAD_VIDEO"})
                try:
                    upload_tiktok_video(page, str(job["video_path"]))
                except PlaywrightTimeoutError as exc:
                    _fail_need_manual(f"Không thấy ô upload file: {exc}", "UPLOAD_VIDEO")
                    return

                patch_job({"step": "WAIT_VIDEO_READY"})
                try:
                    wait_tiktok_video_ready(page)
                except RuntimeError as exc:
                    _fail_retryable(str(exc), "WAIT_VIDEO_READY")
                    return

                patch_job({"step": "INPUT_CAPTION"})
                try:
                    cap = build_tiktok_caption(job)
                    input_tiktok_caption(page, cap)
                except RuntimeError as exc:
                    _fail_retryable(str(exc), "INPUT_CAPTION")
                    return

                patch_job({"step": "APPLY_SETTINGS"})
                apply_tiktok_settings(page, job, log)

                patch_job({"step": "CLICK_POST"})
                try:
                    click_tiktok_post(page)
                except Exception as exc:  # noqa: BLE001
                    _fail_retryable(str(exc), "CLICK_POST")
                    return

                patch_job({"step": "VERIFY_SUCCESS"})
                try:
                    verify_tiktok_post_success(page)
                except RuntimeError as exc:
                    _fail_need_manual(str(exc), "VERIFY_SUCCESS")
                    return

                job["status"] = "completed"
                job["step"] = "DONE"
                job["error_message"] = ""
                job["completed_at"] = datetime.now().replace(microsecond=0).isoformat()
                patch_job(dict(job))
                log("[TikTok] Đã đăng video (hoàn tất).")
            finally:
                try:
                    context.close()
                except Exception as exc:  # noqa: BLE001
                    logger.debug("Đóng TikTok context: {}", exc)

    except Exception as exc:  # noqa: BLE001
        msg = str(exc)
        if page and not check_tiktok_login(page):
            _fail_need_manual("Phiên TikTok có thể đã logout hoặc cần xác minh.", "ERROR")
            return
        save_tiktok_screenshot(page, job)
        _fail_retryable(msg, str(job.get("step") or "ERROR"))


def run_tiktok_login_check_sync(account: dict[str, Any], *, log: LogFn) -> tuple[bool, str]:
    """Mở profile, vào TikTok, kiểm tra đã login (không đổi trạng thái job)."""
    if not str(account.get("profile_path", "")).strip():
        return False, "Thiếu profile_path."
    ctx: BrowserContext | None = None
    try:
        with sync_playwright() as p:
            log("[TikTok] Kiểm tra đăng nhập — mở profile…")
            ctx = open_tiktok_browser(account, playwright=p)
            page = ctx.new_page()
            page.goto("https://www.tiktok.com/", wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(2500)
            if check_tiktok_login(page):
                return True, ""
            return False, "Chưa đăng nhập hoặc TikTok yêu cầu xác minh/captcha."
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)
    finally:
        if ctx:
            try:
                ctx.close()
            except Exception:
                pass
