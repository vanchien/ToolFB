from __future__ import annotations

import contextlib
import json
import hashlib
import mimetypes
import os
import re
import shutil
from urllib.parse import urlparse
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from loguru import logger
from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

from src.services.video_providers.base_video_provider import BaseVideoAIProvider
from src.services.google_flow_veo_generate import (
    check_google_flow_login,
    dismiss_obvious_overlays,
    find_flow_generate_button,
    find_flow_prompt_box,
    is_google_flow_labs_url,
    open_or_create_flow_project,
    select_text_to_video_mode_if_needed,
    verify_prompt_nonempty,
    wait_generate_button_ready,
    wait_flow_generation_done,
)
from src.services.google_flow_text_to_video_runner import GoogleFlowVeoRunner
from src.utils.app_secrets import get_nanobanana_runtime_config
from src.utils.media_dedupe import partition_new_output_files
from src.utils.paths import project_root


class GeminiVideoProvider(BaseVideoAIProvider):
    """
    Provider Veo/Gemini cho AI video.
    """

    def submit_generation(self, request: dict[str, Any]) -> dict[str, Any]:
        """
        Gửi request generate video theo 1 luồng duy nhất: VEO browser queue.
        """
        mode = str(request.get("mode", "")).strip().lower()
        if not mode:
            raise ValueError("Thiếu mode cho Gemini video request.")
        if not _veo_browser_enabled():
            logger.info("VEO3 single-flow: force dùng browser queue dù env chưa bật rõ.")
        op_id = f"veo_browser_{uuid.uuid4().hex[:16]}"
        _save_browser_op(op_id, {"status": "generating", "request": request, "output_files": [], "error_message": ""})
        return {"operation_id": op_id, "status": "generating"}

    def poll_operation(self, operation_id: str) -> dict[str, Any]:
        """
        Poll trạng thái operation từ Gemini.
        """
        if operation_id.startswith("veo_browser_"):
            rec = _load_browser_op(operation_id)
            if not rec:
                return {
                    "operation_id": operation_id,
                    "status": "failed",
                    "error_message": "Không tìm thấy operation browser mode.",
                }
            status = str(rec.get("status", "")).strip().lower()
            if status in {"completed", "failed"}:
                return {
                    "operation_id": operation_id,
                    "status": status,
                    "error_message": str(rec.get("error_message", "") or "").strip(),
                }
            # Chỉ một browser VEO3 / profile tại một thời điểm; nhiều job song song sẽ xếp hàng.
            # Đọc lại trạng thái trong lock để tránh hai luồng poll cùng operation chạy _run hai lần.
            with _veo_profile_browser_lock(_veo_browser_profile_dir()):
                rec2 = _load_browser_op(operation_id)
                if not rec2:
                    return {
                        "operation_id": operation_id,
                        "status": "failed",
                        "error_message": "Không tìm thấy operation browser mode.",
                    }
                st2 = str(rec2.get("status", "")).strip().lower()
                if st2 in {"completed", "failed"}:
                    return {
                        "operation_id": operation_id,
                        "status": st2,
                        "error_message": str(rec2.get("error_message", "") or "").strip(),
                    }
                try:
                    outputs = _run_veo_browser_generation(
                        operation_id=operation_id, request=dict(rec2.get("request") or {})
                    )
                    _save_browser_op(operation_id, {"status": "completed", "output_files": outputs, "error_message": ""})
                    return {"operation_id": operation_id, "status": "completed"}
                except Exception as exc:  # noqa: BLE001
                    _save_browser_op(operation_id, {"status": "failed", "error_message": str(exc)})
                    return {"operation_id": operation_id, "status": "failed", "error_message": str(exc)}
        return {
            "operation_id": operation_id,
            "status": "failed",
            "error_message": "Chỉ hỗ trợ operation browser (veo_browser_*) trong single-flow mode.",
        }

    def download_result(self, operation_id: str, output_dir: str) -> dict[str, Any]:
        """
        Tải toàn bộ output video từ operation đã completed.
        """
        if operation_id.startswith("veo_browser_"):
            rec = _load_browser_op(operation_id)
            files = list((rec or {}).get("output_files") or [])
            status = str((rec or {}).get("status", "")).strip().lower()
            if status != "completed" or not files:
                return {
                    "operation_id": operation_id,
                    "status": "failed",
                    "output_files": [],
                    "error_message": str((rec or {}).get("error_message", "") or "Browser mode chưa có output video."),
                }
            return {
                "operation_id": operation_id,
                "status": "completed",
                "output_files": files,
            }
        return {
            "operation_id": operation_id,
            "status": "failed",
            "output_files": [],
            "error_message": "Chỉ hỗ trợ download từ operation browser (veo_browser_*) trong single-flow mode.",
        }

    def _build_source(self, *, mode: str, prompt: str, assets: dict[str, Any], types: Any):
        source_prompt = prompt or None
        image = None
        video = None

        if mode in {"image_to_video", "image_to_vertical_video", "first_last_frame_to_video"}:
            image_path = str(assets.get("image_path") or assets.get("first_frame_path") or "").strip()
            if not image_path:
                raise ValueError("Mode này cần image_path/first_frame_path.")
            image = _image_from_path(image_path, types)

        if mode == "extend_video":
            source_video = str(assets.get("source_video_path") or "").strip()
            if not source_video:
                raise ValueError("Mode extend_video cần source_video_path.")
            video = _video_from_path(source_video, types)

        if mode == "ingredients_to_video":
            refs = list(assets.get("reference_images") or [])
            if refs and not image:
                image = _image_from_path(str(refs[0]), types)

        return types.GenerateVideosSource(prompt=source_prompt, image=image, video=video)

    def _build_config(self, *, mode: str, opts: dict[str, Any], assets: dict[str, Any], types: Any):
        cfg: dict[str, Any] = {}
        ar = str(opts.get("aspect_ratio", "")).strip()
        if ar:
            cfg["aspectRatio"] = ar
        duration = _to_int_or_none(opts.get("duration_sec"))
        if duration is not None:
            cfg["durationSeconds"] = duration
        count = _to_int_or_none(opts.get("output_count"))
        if count is not None:
            cfg["numberOfVideos"] = count
        resolution = str(opts.get("resolution", "")).strip()
        if resolution:
            cfg["resolution"] = resolution
        seed = _to_int_or_none(opts.get("seed"))
        if seed is not None:
            cfg["seed"] = seed

        if mode == "first_last_frame_to_video":
            last_path = str(assets.get("last_frame_path") or "").strip()
            if last_path:
                cfg["lastFrame"] = _image_from_path(last_path, types)

        if mode in {"prompt_to_vertical_video", "image_to_vertical_video"} and "aspectRatio" not in cfg:
            cfg["aspectRatio"] = "9:16"
        return types.GenerateVideosConfig(**cfg)


def _build_genai_client():
    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("Thiếu GEMINI_API_KEY cho Gemini/Veo video.")
    try:
        from google import genai
        from google.genai import types
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("Thiếu thư viện google-genai. Hãy cài đặt lại dependencies.") from exc
    return genai.Client(api_key=api_key), types


def _to_int_or_none(v: Any) -> int | None:
    try:
        if v is None or str(v).strip() == "":
            return None
        return int(v)
    except Exception:
        return None


def _image_from_path(path: str, types: Any):
    p = Path(path)
    if not p.is_file():
        raise ValueError(f"Không tìm thấy file ảnh: {path}")
    mime = mimetypes.guess_type(p.name)[0] or "image/png"
    return types.Image(imageBytes=p.read_bytes(), mimeType=mime)


def _video_from_path(path: str, types: Any):
    p = Path(path)
    if not p.is_file():
        raise ValueError(f"Không tìm thấy file video: {path}")
    mime = mimetypes.guess_type(p.name)[0] or "video/mp4"
    return types.Video(videoBytes=p.read_bytes(), mimeType=mime)


def _safe_err_text(err: Any) -> str:
    code = ""
    message = ""
    if isinstance(err, dict):
        code = str(err.get("code", "")).strip()
        message = str(err.get("message", "")).strip() or str(err)
    else:
        code = str(getattr(err, "code", "") or "").strip()
        message = str(getattr(err, "message", "") or "").strip() or str(err)
    return f"{code}: {message}".strip(": ").strip()


def _guess_video_suffix(video_obj: Any) -> str:
    mime = str(getattr(video_obj, "mime_type", "") or getattr(video_obj, "mimeType", "") or "").strip().lower()
    if "webm" in mime:
        return ".webm"
    if "quicktime" in mime or "mov" in mime:
        return ".mov"
    return ".mp4"


def _veo_browser_enabled() -> bool:
    raw = os.environ.get("VEO3_USE_BROWSER", "").strip().lower()
    if raw:
        return raw not in {"0", "false", "off", "no"}
    raw2 = os.environ.get("NANOBANANA_USE_BROWSER", "0").strip().lower()
    if raw2 not in {"0", "false", "off", "no"}:
        return True
    # Fallback: nếu đã login profile sẵn thì auto dùng browser mode.
    prof = _veo_browser_profile_dir()
    has_profile = any(prof.iterdir()) if prof.is_dir() else False
    if has_profile:
        return True
    cfg = get_nanobanana_runtime_config()
    return bool(str(cfg.get("web_url", "")).strip())


# Trình tạo video Veo trong browser dùng Google Flow (labs), không dùng Gemini chat làm mặc định.
_VEO_BROWSER_DEFAULT_FLOW_URL = "https://labs.google/fx/vi/tools/flow"


def _veo_browser_web_url() -> str:
    """
    URL mở khi chạy VEO3 browser mode.

    Ưu tiên VEO3_WEB_URL, sau đó NANOBANANA_WEB_URL / cấu hình app.
    Nếu URL vẫn trỏ tới gemini.google.com (thường do đồng bộ với NanoBanana),
    tự động dùng Google Flow — trừ khi bật VEO3_USE_GEMINI_CHAT_URL=1.
    """
    cfg = get_nanobanana_runtime_config()
    u = (
        os.environ.get("VEO3_WEB_URL", "").strip()
        or os.environ.get("NANOBANANA_WEB_URL", "").strip()
        or str(cfg.get("web_url", "")).strip()
        or _VEO_BROWSER_DEFAULT_FLOW_URL
    )
    allow_gemini = os.environ.get("VEO3_USE_GEMINI_CHAT_URL", "").strip().lower() in {"1", "true", "yes", "on"}
    if "gemini.google.com" in u.lower() and not allow_gemini:
        logger.info(
            "VEO3 browser: bỏ qua URL Gemini chat ({}), mở Google Flow để tạo video.",
            u,
        )
        return _VEO_BROWSER_DEFAULT_FLOW_URL
    return u


def _veo_browser_profile_dir() -> Path:
    p = os.environ.get("VEO3_BROWSER_PROFILE_DIR", "").strip() or os.environ.get("NANOBANANA_BROWSER_PROFILE_DIR", "").strip()
    d = Path(p) if p else (project_root() / "data" / "nanobanana" / "browser_profile")
    d.mkdir(parents=True, exist_ok=True)
    return d.resolve()


_veo_browser_profile_locks: dict[str, threading.Lock] = {}
_veo_browser_profile_locks_guard = threading.Lock()


@contextlib.contextmanager
def _veo_profile_browser_lock(profile: Path):
    """
    Khóa theo đường dẫn profile: nhiều job AI video (nhiều dòng prompt) không được
    launch_persistent_context đồng thời trên cùng user_data_dir (Chrome thoát 21).
    """
    key = str(profile.resolve())
    with _veo_browser_profile_locks_guard:
        lock = _veo_browser_profile_locks.setdefault(key, threading.Lock())
    lock.acquire()
    try:
        yield
    finally:
        lock.release()


def _browser_ops_store_path() -> Path:
    p = project_root() / "data" / "ai_video" / "temp" / "veo_browser_ops.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    if not p.is_file():
        p.write_text("{}\n", encoding="utf-8")
    return p


def _load_browser_ops() -> dict[str, Any]:
    p = _browser_ops_store_path()
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        raw = {}
    return raw if isinstance(raw, dict) else {}


def _save_browser_op(operation_id: str, patch: dict[str, Any]) -> None:
    all_ops = _load_browser_ops()
    row = dict(all_ops.get(operation_id) or {})
    row.update(patch)
    all_ops[operation_id] = row
    _browser_ops_store_path().write_text(json.dumps(all_ops, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _load_browser_op(operation_id: str) -> dict[str, Any] | None:
    all_ops = _load_browser_ops()
    row = all_ops.get(operation_id)
    return dict(row) if isinstance(row, dict) else None


def _character_bible_store_path() -> Path:
    p = project_root() / "data" / "ai_video" / "temp" / "character_bibles.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    if not p.is_file():
        p.write_text("{}\n", encoding="utf-8")
    return p


def _load_character_bibles() -> dict[str, Any]:
    p = _character_bible_store_path()
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        raw = {}
    return raw if isinstance(raw, dict) else {}


def _save_character_bible(lock_id: str, bible: dict[str, Any]) -> None:
    lock = str(lock_id or "").strip()
    if not lock:
        return
    all_rows = _load_character_bibles()
    all_rows[lock] = dict(bible)
    _character_bible_store_path().write_text(json.dumps(all_rows, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _build_browser_consistency_prompt(*, request: dict[str, Any]) -> str:
    base_prompt = str(request.get("prompt", "")).strip()
    prof = request.get("character_profile")
    profile = dict(prof) if isinstance(prof, dict) else {}
    lock_id = (
        str(request.get("character_lock_id", "")).strip()
        or str(profile.get("character_lock_id", "")).strip()
        or _fallback_lock_id(base_prompt=base_prompt, profile=profile)
    )
    if not lock_id:
        return base_prompt

    current_bible = {
        "character_name": str(profile.get("character_name", "")).strip(),
        "character_description": str(profile.get("character_description", "")).strip(),
        "outfit": str(profile.get("outfit", "")).strip(),
        "facial_features": str(profile.get("facial_features", "")).strip(),
        "personality": str(profile.get("personality", "")).strip(),
        "supporting_characters": list(profile.get("supporting_characters") or []),
    }
    saved = _load_character_bibles().get(lock_id)
    bible = dict(saved) if isinstance(saved, dict) and saved else dict(current_bible)
    if not saved:
        _save_character_bible(lock_id, bible)

    support_lines = [
        f"- {str(x).strip()}"
        for x in list(bible.get("supporting_characters") or [])
        if str(x).strip()
    ]
    support_text = "\n".join(support_lines) if support_lines else "- None"
    guard_block = (
        "Browser Character Bible (strict continuity):\n"
        f"LOCK_ID: {lock_id}\n"
        f"Main character name: {str(bible.get('character_name', '')).strip() or 'Main Character'}\n"
        f"Main appearance: {str(bible.get('character_description', '')).strip() or 'keep same appearance identity'}\n"
        f"Facial signature: {str(bible.get('facial_features', '')).strip() or 'keep same face geometry'}\n"
        f"Outfit signature: {str(bible.get('outfit', '')).strip() or 'keep same outfit palette and style'}\n"
        f"Personality: {str(bible.get('personality', '')).strip() or 'keep same personality'}\n"
        "Supporting cast:\n"
        f"{support_text}\n"
        "Hard continuity constraints:\n"
        "- Do not recast, replace, morph, or randomize any character under this LOCK_ID.\n"
        "- Keep same face geometry, skin tone, hair style, age impression, body shape, outfit palette.\n"
        "- Keep supporting cast identity and role mapping unchanged.\n"
        "- If conflict occurs, identity consistency overrides cinematic variation.\n"
    )
    if "Browser Character Bible (strict continuity):" in base_prompt:
        return base_prompt
    return f"{guard_block}\n{base_prompt}".strip()


def _fallback_lock_id(*, base_prompt: str, profile: dict[str, Any]) -> str:
    base = "|".join(
        [
            str(profile.get("character_name", "")).strip().lower(),
            str(profile.get("character_description", "")).strip().lower(),
            str(profile.get("outfit", "")).strip().lower(),
            str(profile.get("facial_features", "")).strip().lower(),
            str(base_prompt or "").strip().lower()[:200],
        ]
    )
    if not base.strip("|"):
        return ""
    digest = hashlib.sha1(base.encode("utf-8")).hexdigest()[:12]
    return f"char-{digest}"


def _wait_for_next_queue_video_or_flow_done(
    *,
    page: Any,
    seen_norm_urls: frozenset[str],
    wait_ms: int,
    prompt_index: int,
) -> None:
    """
    Prompt đầu: chờ Flow báo xong (video + download, không generating).

    Prompt 2+ trong ``prompt_queue``: Flow có thể vẫn hiện generating cho gen khác nên chờ full rất lâu;
    ưu tiên phát hiện **URL video mới** trên DOM (chưa nằm trong ``seen_norm_urls``), rồi mới tải.
    """
    if prompt_index <= 1:
        wait_flow_generation_done(page, timeout_ms=max(60_000, wait_ms))
        return
    # Giảm thời gian "đợi URL mới" để tránh đứng lâu ở prompt 2/3.
    # Nếu không thấy tín hiệu mới sớm thì chuyển ngay sang nhánh chờ generation done ổn định hơn.
    fast_budget = max(20.0, min(75.0, wait_ms / 8000.0))
    end_fast = time.time() + fast_budget
    while time.time() < end_fast:
        for u in _extract_video_urls(page):
            nu = _url_norm_for_dedupe(u)
            if nu and nu not in seen_norm_urls:
                try:
                    page.wait_for_timeout(320)
                except Exception:
                    time.sleep(0.32)
                logger.info(
                    "VEO3 browser: prompt {} — có URL video mới trên DOM, không chờ full UI «done».",
                    prompt_index,
                )
                return
        try:
            page.wait_for_timeout(420)
        except Exception:
            time.sleep(0.42)
    logger.info(
        "VEO3 browser: prompt {} — không thấy URL mới trong {:.0f}s, fallback chờ Flow generation done.",
        prompt_index,
        fast_budget,
    )
    wait_flow_generation_done(page, timeout_ms=max(60_000, wait_ms))


def _run_veo_browser_generation(*, operation_id: str, request: dict[str, Any]) -> list[str]:
    base_prompt = _build_browser_consistency_prompt(request=request)
    if not base_prompt:
        raise RuntimeError("Browser mode yêu cầu prompt không rỗng.")
    prompts = _extract_browser_prompt_queue(request=request, fallback_prompt=base_prompt)
    if not prompts:
        raise RuntimeError("Không có prompt hợp lệ để chạy browser mode.")
    requested_model = str(request.get("model", "")).strip() or str(dict(request.get("options") or {}).get("model", "")).strip()
    settings = dict(request.get("options") or {})
    preferred_resolution = _normalize_resolution_label(str(settings.get("resolution", "")).strip())
    raw_out_dir = str(settings.get("output_dir", "") or "").strip()
    if raw_out_dir:
        out_dir = Path(raw_out_dir).expanduser().resolve()
    else:
        pid = str(request.get("project_id", "") or "").strip()
        out_dir = (project_root() / "data" / "ai_video" / "outputs" / pid) if pid else (project_root() / "data" / "ai_video" / "outputs")
    out_dir.mkdir(parents=True, exist_ok=True)
    wait_ms = int(float(os.environ.get("VEO3_WAIT_MS", "420000")))
    manual_login_sec = int(float(os.environ.get("VEO3_MANUAL_LOGIN_SEC", "30")))
    action_delay_ms = int(float(os.environ.get("VEO3_ACTION_DELAY_MS", "1200")))
    profile = _veo_browser_profile_dir()
    web_url = _veo_browser_web_url()

    _cleanup_profile_lock_files(profile)

    with sync_playwright() as pw:
        context = _launch_persistent_context_with_retry(pw=pw, profile=profile)
        try:
            try:
                Stealth().apply_stealth_sync(context)
            except Exception:
                pass
            page = context.pages[0] if context.pages else context.new_page()
            page.goto(web_url, wait_until="domcontentloaded", timeout=60_000)
            if is_google_flow_labs_url(web_url):
                # Flow URL: nếu đã login thì vào project/mode luôn, không chờ login tay vô ích.
                if not check_google_flow_login(page):
                    logger.info("VEO3 browser: chưa thấy login hợp lệ, chờ login tay {}s...", manual_login_sec)
                    time.sleep(max(5, manual_login_sec))
                    try:
                        page.goto(web_url, wait_until="domcontentloaded", timeout=60_000)
                    except Exception:
                        pass
                # Flow Home -> vào project workspace trước khi nhập prompt.
                open_or_create_flow_project(page)
                select_text_to_video_mode_if_needed(page, requested_model=requested_model)
                # Áp cài đặt theo lựa chọn user (model/aspect/outputs/duration) trước khi submit queue.
                try:
                    if requested_model and not settings.get("model"):
                        settings["model"] = requested_model
                    GoogleFlowVeoRunner().apply_settings(page, settings=settings)
                    logger.info("VEO3 browser: đã áp xong settings, chờ UI ổn định rồi mới nhập prompt.")
                    if action_delay_ms > 0:
                        page.wait_for_timeout(max(1400, action_delay_ms))
                except Exception as exc:
                    logger.warning("VEO3 browser: không áp đủ settings theo cấu hình user: {}", exc)
            # Với non-Flow fallback: đợi prompt sau cùng.
            if not is_google_flow_labs_url(web_url) and not _wait_prompt_box(page, timeout_sec=12):
                logger.info("VEO3 browser: chưa thấy prompt, chờ login tay {}s...", manual_login_sec)
                time.sleep(max(5, manual_login_sec))
                try:
                    page.goto(web_url, wait_until="domcontentloaded", timeout=60_000)
                except Exception:
                    pass
            # Phase A: submit nhanh toàn bộ prompt (xác nhận click xong là qua prompt kế tiếp).
            for idx, prompt in enumerate(prompts, start=1):
                t_submit = time.time()
                logger.info("VEO3 browser: submit prompt {}/{} ...", idx, len(prompts))
                _set_prompt(page, prompt[:1800])
                if action_delay_ms > 0:
                    page.wait_for_timeout(max(300, action_delay_ms))
                if is_google_flow_labs_url(web_url):
                    try:
                        # Submit-only: chỉ cần click generate thành công là qua prompt kế tiếp.
                        # Tránh chặn cứng do tín hiệu "started" thay đổi theo UI từng tài khoản.
                        dismiss_obvious_overlays(page)
                        prompt_box = find_flow_prompt_box(page)
                        verify_prompt_nonempty(prompt_box)
                        button = find_flow_generate_button(page, prompt_box)
                        wait_generate_button_ready(button)
                        button.click(timeout=6000)
                        logger.info("VEO3 browser: prompt {} đã click tạo thành công, chuyển prompt kế tiếp.", idx)
                        if action_delay_ms > 0:
                            page.wait_for_timeout(max(500, action_delay_ms))
                    except Exception as exc:
                        _save_debug_screenshot(page, reason=f"{operation_id}_send_failed_{idx}")
                        raise RuntimeError(
                            f"Prompt {idx}: không bấm được / không xác nhận được tạo video trên Google Flow (Labs)."
                        ) from exc
                elif not _click_send(page):
                    _save_debug_screenshot(page, reason=f"{operation_id}_send_failed_{idx}")
                    raise RuntimeError(f"Prompt {idx}: không bấm được nút Send/Submit trên giao diện Gemini/Veo3.")
                logger.info(
                    "VEO3 browser timing: prompt {}/{} submit {:.2f}s",
                    idx,
                    len(prompts),
                    time.time() - t_submit,
                )

            # Phase B: đợi hoàn tất và download theo thứ tự prompt 1..N.
            # Tránh tái tải cùng một URL / cùng nội dung file khi DOM vẫn còn video gen trước (Flow hay giữ nhiều thẻ).
            all_outputs: list[str] = []
            seen_src_urls: set[str] = set()
            seen_file_hashes: set[str] = set()
            for idx in range(1, len(prompts) + 1):
                t_wait = time.time()
                logger.info("VEO3 browser: chờ hoàn tất prompt {}/{} để tải video...", idx, len(prompts))
                if is_google_flow_labs_url(web_url):
                    _wait_for_next_queue_video_or_flow_done(
                        page=page,
                        seen_norm_urls=frozenset(seen_src_urls),
                        wait_ms=wait_ms,
                        prompt_index=idx,
                    )
                wait_elapsed = time.time() - t_wait
                t_dl = time.time()
                outputs = _wait_and_collect_videos(
                    page=page,
                    output_dir=out_dir,
                    operation_id=f"{operation_id}_p{idx:02d}",
                    timeout_sec=max(60, wait_ms // 1000),
                    preferred_resolution=preferred_resolution,
                    seen_src_urls=seen_src_urls,
                )
                outputs = _consume_unseen_output_files(paths=outputs, seen_hashes=seen_file_hashes)
                if not outputs:
                    _save_debug_screenshot(page, reason=f"{operation_id}_no_output_{idx}")
                    raise RuntimeError(
                        f"Prompt {idx}: không lấy được video output mới từ browser "
                        f"(có thể UI chưa sinh clip mới hoặc clip trùng với prompt trước)."
                    )
                dl_elapsed = time.time() - t_dl
                all_outputs.extend(outputs)
                logger.info("VEO3 browser: prompt {} đã tải {} file (sau lọc trùng).", idx, len(outputs))
                logger.info(
                    "VEO3 browser timing: prompt {}/{} wait {:.2f}s | download {:.2f}s | total {:.2f}s",
                    idx,
                    len(prompts),
                    wait_elapsed,
                    dl_elapsed,
                    wait_elapsed + dl_elapsed,
                )
            return all_outputs
        finally:
            try:
                context.close()
            except Exception:
                pass


def _extract_browser_prompt_queue(*, request: dict[str, Any], fallback_prompt: str) -> list[str]:
    """
    Lấy danh sách prompt theo thứ tự:
    - options.prompt_queue / request.prompt_queue (list[str]) nếu có
    - ngược lại dùng prompt hiện tại (1 phần tử)
    """
    out: list[str] = []
    opts = dict(request.get("options") or {})
    raw_queue = opts.get("prompt_queue")
    if not isinstance(raw_queue, list):
        raw_queue = request.get("prompt_queue")
    if isinstance(raw_queue, list):
        for x in raw_queue:
            s = str(x or "").strip()
            if s:
                out.append(s)
    if not out:
        s = str(fallback_prompt or "").strip()
        if s:
            out.append(s)
    return out


def _normalize_resolution_label(raw: str) -> str:
    s = str(raw or "").strip().lower().replace(" ", "")
    if not s:
        return ""
    if s in {"4k", "2160p", "uhd"}:
        return "4K"
    if s in {"1080", "1080p", "fullhd", "fhd", "1920x1080"}:
        return "1080p"
    if s in {"720", "720p", "hd", "1280x720"}:
        return "720p"
    return ""


def _url_norm_for_dedupe(url: str) -> str:
    """
    Chuẩn hóa URL video để tránh tải lặp.
    Giữ cả query-string vì một số phiên Flow dùng cùng path nhưng query khác cho clip mới.
    """
    u = str(url or "").strip()
    if not u:
        return ""
    try:
        p = urlparse(u)
        query = f"?{p.query}" if p.query else ""
        base = f"{p.scheme}://{p.netloc}{p.path}{query}".lower().rstrip("/")
        return base or u.lower()
    except Exception:
        return u.lower()


def _consume_unseen_output_files(*, paths: list[str], seen_hashes: set[str]) -> list[str]:
    """
    Giữ các file output chưa xuất hiện (theo fingerprint); xóa file trùng nội dung với prompt trước.
    """
    n_in = len(paths)
    kept = partition_new_output_files(paths, seen_hashes, delete_duplicate_files=True)
    if n_in > len(kept):
        logger.info("VEO3 browser: lọc trùng nội dung {} -> {} file.", n_in, len(kept))
    return kept


def _launch_persistent_context_with_retry(*, pw: Any, profile: Path):
    channel = str(os.environ.get("VEO3_CHROMIUM_CHANNEL", "chrome")).strip().lower()
    use_channel = bool(channel and channel not in {"0", "false", "off", "bundled", "playwright", "chromium"})
    attempts: list[tuple[str, list[str]]] = [
        (
            "default",
            ["--lang=en-US"],
        ),
        (
            "reduced-flags",
            ["--lang=en-US", "--disable-extensions"],
        ),
        (
            "minimal",
            [],
        ),
    ]
    last_exc: Exception | None = None
    for tag, extra_args in attempts:
        try:
            _cleanup_profile_lock_files(profile)
            launch_kwargs: dict[str, Any] = dict(
                user_data_dir=str(profile),
                headless=False,
                viewport={"width": 1366, "height": 900},
                locale="en-US",
                args=extra_args,
                accept_downloads=True,
            )
            if use_channel:
                launch_kwargs["channel"] = channel
            ctx = pw.chromium.launch_persistent_context(**launch_kwargs)
            logger.info("VEO3 browser launch OK [{}] profile={}", tag, profile)
            return ctx
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            logger.warning("VEO3 browser launch failed [{}]: {}", tag, exc)
            time.sleep(0.8)
            continue
    # Mặc định KHÔNG tự fallback sang recovery để tránh mở thêm browser/profile
    # khi người dùng đã có profile chính đăng nhập sẵn.
    # Chỉ bật khi cần bằng env: VEO3_ENABLE_RECOVERY_PROFILE=1
    allow_recovery = os.environ.get("VEO3_ENABLE_RECOVERY_PROFILE", "").strip().lower() in {"1", "true", "yes", "on"}
    if not allow_recovery:
        raise RuntimeError(
            "Không mở được browser profile đã login. "
            "Hãy bấm «Reset profile VEO3» trong GUI rồi thử lại "
            "(hoặc bật VEO3_ENABLE_RECOVERY_PROFILE=1 nếu muốn auto fallback)."
        ) from last_exc

    # Fallback an toàn: nếu profile chính bị lỗi/corrupt, thử profile recovery sạch.
    recovery_profile = _recovery_profile_dir(profile)
    logger.warning(
        "VEO3 browser: fallback sang profile recovery do profile chính launch thất bại. profile={} recovery={}",
        profile,
        recovery_profile,
    )
    _cleanup_profile_lock_files(recovery_profile)
    for tag, extra_args in attempts:
        rec_tag = f"recovery-{tag}"
        try:
            launch_kwargs: dict[str, Any] = dict(
                user_data_dir=str(recovery_profile),
                headless=False,
                viewport={"width": 1366, "height": 900},
                locale="en-US",
                args=extra_args,
                accept_downloads=True,
            )
            if use_channel:
                launch_kwargs["channel"] = channel
            ctx = pw.chromium.launch_persistent_context(**launch_kwargs)
            logger.info("VEO3 browser launch OK [{}] profile={}", rec_tag, recovery_profile)
            return ctx
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            logger.warning("VEO3 browser launch failed [{}]: {}", rec_tag, exc)
            time.sleep(0.8)
            continue
    raise RuntimeError(
        "Không mở được browser profile đã login. "
        "Đã thử cả profile recovery nhưng vẫn lỗi. "
        "Hãy đóng toàn bộ Chrome/Chromium và thử lại."
    ) from last_exc


def _cleanup_profile_lock_files(profile: Path) -> None:
    """
    Dọn lock file còn sót của Chromium profile để tránh crash launch_persistent_context.
    """
    names = ("SingletonLock", "SingletonCookie", "SingletonSocket", "lockfile")
    for name in names:
        p = profile / name
        try:
            if p.exists():
                p.unlink()
        except Exception:
            continue


def _recovery_profile_dir(profile: Path) -> Path:
    """
    Trả về profile recovery cùng cấp để fallback khi profile chính crash lúc launch.
    """
    parent = profile.parent
    name = f"{profile.name}_recovery"
    rec = (parent / name).resolve()
    rec.mkdir(parents=True, exist_ok=True)
    # Marker giúp người dùng biết đây là profile fallback riêng.
    marker = rec / "TOOLFB_RECOVERY_PROFILE.txt"
    if not marker.exists():
        try:
            marker.write_text(
                "Profile fallback cua ToolFB khi profile chinh launch that bai.\n",
                encoding="utf-8",
            )
        except Exception:
            pass
    # Dọn lock rác nếu còn.
    _cleanup_profile_lock_files(rec)
    # Nếu recovery quá lớn/bẩn bất thường, có thể reset nhanh để giảm crash liên hoàn.
    try:
        if (rec / "SingletonLock").exists():
            (rec / "SingletonLock").unlink(missing_ok=True)
    except Exception:
        pass
    return rec


def _set_prompt(page: Any, prompt: str) -> None:
    sels = [
        "textarea[aria-label*='message' i]",
        "textarea[aria-label*='prompt' i]",
        "textarea",
        "[role='textbox'][contenteditable='true']",
    ]
    for sel in sels:
        try:
            box = page.locator(sel).first
            if not box.is_visible():
                continue
            try:
                box.fill("")
            except Exception:
                pass
            try:
                box.fill(prompt)
            except Exception:
                box.click(timeout=2000)
                page.keyboard.press("Control+A")
                page.keyboard.type(prompt, delay=8)
            return
        except Exception:
            continue
    raise RuntimeError("Không tìm thấy ô prompt trong giao diện Gemini/Veo3.")


def _wait_prompt_box(page: Any, *, timeout_sec: int = 12) -> bool:
    sels = [
        "textarea[aria-label*='message' i]",
        "textarea[aria-label*='prompt' i]",
        "textarea",
        "[role='textbox'][contenteditable='true']",
    ]
    deadline = time.time() + max(3, timeout_sec)
    while time.time() < deadline:
        for sel in sels:
            try:
                if page.locator(sel).first.is_visible():
                    return True
            except Exception:
                continue
        time.sleep(0.4)
    return False


def _click_send(page: Any) -> bool:
    sels = [
        "button.send-button.submit[aria-label='Send message'][aria-disabled='false']",
        "button.send-button[aria-label='Send message'][aria-disabled='false']",
        "button[aria-label='Send message']",
        "button[aria-label='Submit']",
        "button:has-text('Submit')",
    ]
    for sel in sels:
        try:
            btn = page.locator(sel).first
            if not btn.is_visible():
                continue
            btn.click(timeout=3500)
            return True
        except Exception:
            continue
    try:
        page.keyboard.press("Control+Enter")
        return True
    except Exception:
        return False


def _wait_and_collect_videos(
    *,
    page: Any,
    output_dir: Path,
    operation_id: str,
    timeout_sec: int,
    preferred_resolution: str = "",
    seen_src_urls: set[str],
) -> list[str]:
    deadline = time.time() + max(40, int(timeout_sec))
    files: list[str] = []
    saw_video_node = False
    while time.time() < deadline and not files:
        files.extend(
            _try_download_videos(
                page=page,
                output_dir=output_dir,
                operation_id=operation_id,
                preferred_resolution=preferred_resolution,
            )
        )
        if files:
            break
        urls = _extract_video_urls(page)
        if urls:
            saw_video_node = True
        files.extend(
            _download_video_urls(
                page=page,
                urls=urls,
                output_dir=output_dir,
                operation_id=operation_id,
                seen_src_urls=seen_src_urls,
            )
        )
        if files:
            break
        # nếu đã thấy video node thì ưu tiên chờ thêm chút để nút download hiện ra.
        if saw_video_node:
            time.sleep(0.65)
            continue
        time.sleep(0.85)
    # unique + existing
    out: list[str] = []
    seen: set[str] = set()
    for f in files:
        sf = str(f)
        if sf in seen:
            continue
        if Path(sf).is_file():
            seen.add(sf)
            out.append(sf)
    return out


def _try_download_videos(*, page: Any, output_dir: Path, operation_id: str, preferred_resolution: str = "") -> list[str]:
    sels = [
        "button:has(mat-icon[fonticon='download'])",
        "[role='button']:has(mat-icon[fonticon='download'])",
        "button[aria-label*='download' i]",
        "[role='button'][aria-label*='download' i]",
        "button:has-text('Download')",
    ]
    out: list[str] = []
    for i, sel in enumerate(sels, start=1):
        try:
            btn = page.locator(sel).first
            if not btn.is_visible():
                continue
            if preferred_resolution:
                p = _try_download_with_resolution_option(
                    page=page,
                    trigger=btn,
                    operation_id=operation_id,
                    output_dir=output_dir,
                    preferred_resolution=preferred_resolution,
                    idx=i,
                )
                if p:
                    out.append(p)
                    continue
            with page.expect_download(timeout=6000) as dlinfo:
                btn.click(timeout=2500)
            dl = dlinfo.value
            p = dl.path()
            data = Path(p).read_bytes() if p else b""
            if not data:
                continue
            fp = output_dir / f"{operation_id}_dl_{i:02d}.mp4"
            fp.write_bytes(data)
            out.append(str(fp))
        except Exception:
            continue
    return out


def _try_download_with_resolution_option(
    *,
    page: Any,
    trigger: Any,
    operation_id: str,
    output_dir: Path,
    preferred_resolution: str,
    idx: int,
) -> str:
    """
    Mở menu tải xuống và chọn đúng option độ phân giải (720p/1080p/4K) nếu có.
    Trả về đường dẫn file đã tải, hoặc chuỗi rỗng nếu không tải được theo nhánh này.
    """
    target = _normalize_resolution_label(preferred_resolution)
    if not target:
        return ""
    try:
        trigger.click(timeout=2500)
        page.wait_for_timeout(350)
    except Exception:
        return ""
    # Một số UI yêu cầu bấm menu "Tải xuống/Download" trước khi hiện options độ phân giải.
    download_menu_selectors = (
        "button[role='menuitem']:has-text('Tải xuống')",
        "button[role='menuitem']:has-text('Download')",
        "[role='menuitem']:has-text('Tải xuống')",
        "[role='menuitem']:has-text('Download')",
    )
    for sel in download_menu_selectors:
        try:
            item = page.locator(sel).first
            if item.count() > 0 and item.is_visible(timeout=350):
                item.click(timeout=1500)
                page.wait_for_timeout(350)
                break
        except Exception:
            continue
    # Chọn option độ phân giải và bắt download.
    option_selectors = (
        f"[role='menuitem']:has-text('{target}')",
        f"button[role='menuitem']:has-text('{target}')",
        f"button:has-text('{target}')",
        f"text=/{re.escape(target)}/i",
    )
    for sel in option_selectors:
        try:
            opt = page.locator(sel).first
            if opt.count() == 0 or not opt.is_visible(timeout=500):
                continue
            with page.expect_download(timeout=12000) as dlinfo:
                opt.click(timeout=2500)
            dl = dlinfo.value
            p = dl.path()
            data = Path(p).read_bytes() if p else b""
            if not data:
                continue
            fp = output_dir / f"{operation_id}_{target}_dl_{idx:02d}.mp4"
            fp.write_bytes(data)
            logger.info("VEO3 browser: đã tải video theo upscale {}", target)
            return str(fp)
        except Exception:
            continue
    return ""


def _extract_video_urls(page: Any) -> list[str]:
    script = """
(() => {
  const out = [];
  for (const v of Array.from(document.querySelectorAll('video'))) {
    const src = (v.currentSrc || v.src || '').trim();
    if (src && !src.startsWith('blob:')) out.push(src);
    for (const s of Array.from(v.querySelectorAll('source'))) {
      const u = (s.src || '').trim();
      if (u && !u.startsWith('blob:')) out.push(u);
    }
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
    return [str(x).strip() for x in got if str(x).strip()]


def _download_video_urls(
    *,
    page: Any,
    urls: list[str],
    output_dir: Path,
    operation_id: str,
    seen_src_urls: set[str],
) -> list[str]:
    """
    Chỉ tải các URL video chưa gặp; nếu nhiều URL mới, chỉ lấy URL cuối (gen mới nhất trong DOM).
    """
    fresh: list[tuple[str, str]] = []
    for u in urls:
        nu = _url_norm_for_dedupe(u)
        if not nu or nu in seen_src_urls:
            continue
        fresh.append((nu, str(u).strip()))
    if fresh:
        norm_pick, raw_pick = fresh[-1]
    else:
        # Fallback cho Flow queue: có trường hợp clip mới vẫn dùng URL cũ (hoặc path cũ),
        # nếu chặn tuyệt đối theo seen URL sẽ chỉ tải được prompt đầu tiên.
        raw_pick = str(urls[-1]).strip() if urls else ""
        norm_pick = _url_norm_for_dedupe(raw_pick)
        if not raw_pick:
            return []
    out: list[str] = []
    try:
        resp = page.context.request.get(raw_pick, timeout=45_000)
        if not resp.ok:
            return []
        ctype = str(resp.headers.get("content-type", "")).lower()
        data = bytes(resp.body())
        if not data:
            return []
        if "video" not in ctype and not re.search(r"\.(mp4|webm|mov)(\?|$)", raw_pick, flags=re.I):
            return []
        ext = ".webm" if "webm" in ctype or ".webm" in raw_pick.lower() else ".mp4"
        fp = output_dir / f"{operation_id}_url_01{ext}"
        fp.write_bytes(data)
        seen_src_urls.add(norm_pick)
        out.append(str(fp))
    except Exception:
        return []
    return out


def _save_debug_screenshot(page: Any, *, reason: str) -> None:
    try:
        d = project_root() / "logs" / "screenshots"
        d.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        name = re.sub(r"[^a-zA-Z0-9_-]+", "_", reason).strip("_")[:80] or "veo3_err"
        p = d / f"veo3_{name}_{ts}.png"
        page.screenshot(path=str(p), full_page=True)
        logger.warning("Đã chụp screenshot debug VEO3 browser: {}", p)
    except Exception:
        pass

