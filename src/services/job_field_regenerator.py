"""
Service sinh lại **chỉ** các field thiếu trên job ``schedule_posts``.

Nguyên tắc:
- Chỉ tạo lại field thực sự thiếu, hoặc field user chỉ định.
- Không ghi đè các field đã có dữ liệu hợp lệ.
- Xử lý đúng thứ tự dependency: title → content → hashtags → image_prompt → image_path.
- Bảo vệ các field core (``scheduled_at``, ``page_id``, ``account_id``, ``post_type``) —
  không bao giờ patch chúng.

Usage:
    from src.services.job_field_regenerator import regenerate_missing_fields_for_job
    updated, regen = regenerate_missing_fields_for_job(job)
"""

from __future__ import annotations

import os
import uuid
from typing import Any, Iterable

from loguru import logger

from src.utils.schedule_posts_missing_fields import (
    CRITICAL_FIELDS,
    REGENERABLE_FIELDS,
    IMAGE_SUFFIXES,
    get_missing_fields,
    order_regenerable_fields,
)
from src.services.ai_image_service import AIImageService
from src.services.ai_text_service import AITextService

PROTECTED_FIELDS: frozenset[str] = frozenset(CRITICAL_FIELDS)


def _language_label(job: dict[str, Any]) -> str:
    v = str(job.get("ai_language", "") or "").strip()
    return v or "Tiếng Việt"


def _text_provider(job: dict[str, Any]) -> str:
    return str(job.get("ai_provider_text", "") or os.environ.get("AI_PROVIDER_TEXT", "gemini")).strip().lower()


def _image_provider(job: dict[str, Any]) -> str:
    return str(job.get("ai_provider_image", "") or os.environ.get("AI_PROVIDER_IMAGE", "gemini")).strip().lower()


def _text_model(job: dict[str, Any]) -> str | None:
    v = str(job.get("ai_model_text", "") or "").strip()
    return v or None


def _image_model(job: dict[str, Any]) -> str | None:
    v = str(job.get("ai_model_image", "") or "").strip()
    return v or None


def _effective_topic(job: dict[str, Any]) -> str:
    """Lấy topic để làm seed khi cần sinh title/content từ đầu."""
    for k in ("ai_topic", "title", "image_alt", "content"):
        v = str(job.get(k, "") or "").strip()
        if v:
            return v[:280]
    return ""


def _truncate(s: str, n: int) -> str:
    s = s or ""
    return s if len(s) <= n else s[: n - 1].rstrip() + "…"


def _regen_title(job: dict[str, Any]) -> str:
    lang = _language_label(job)
    topic = _effective_topic(job)
    out = AITextService().generate_title(
        topic=_truncate(topic, 300),
        language=lang,
        provider=_text_provider(job),
        model=_text_model(job),
    )
    out = out.split("\n", 1)[0].strip().strip('"').strip("'").rstrip(".,;:")
    if len(out) > 200:
        out = out[:200].rsplit(" ", 1)[0]
    return out


def _regen_content(job: dict[str, Any]) -> tuple[str, str]:
    """Trả ``(body, image_alt)`` — image_alt dùng cho image_prompt sau này."""
    lang = _language_label(job)
    topic = _effective_topic(job) or str(job.get("title", "") or "").strip()
    style = str(job.get("ai_content_style", "") or "").strip() or "thân mật, tự nhiên"
    out = AITextService().generate_post(
        topic=topic,
        style=style,
        language=lang,
        provider=_text_provider(job),
        model=_text_model(job),
    )
    body = str(out.get("body", "")).strip()
    alt = str(out.get("image_alt", "")).strip()
    if not body:
        raise RuntimeError("Gemini không trả được body.")
    return body, alt


def _regen_hashtags(job: dict[str, Any], *, n: int = 6) -> list[str]:
    lang = _language_label(job)
    title = str(job.get("title", "") or "").strip()
    body = str(job.get("content", "") or "").strip()
    topic = _effective_topic(job)
    tags = AITextService().generate_hashtags(
        title=title or topic,
        body=body,
        language=lang,
        count=n,
        provider=_text_provider(job),
        model=_text_model(job),
    )
    if not tags:
        raise RuntimeError("Gemini không trả được hashtags.")
    return tags[:n]


def _regen_cta(job: dict[str, Any]) -> str:
    lang = _language_label(job)
    topic = _effective_topic(job) or str(job.get("title", "") or "").strip()
    out = AITextService().generate_cta(
        topic=_truncate(topic, 320),
        language=lang,
        provider=_text_provider(job),
        model=_text_model(job),
    )
    return out[:300]


def _regen_image_prompt(job: dict[str, Any]) -> str:
    title = str(job.get("title", "") or "").strip()
    body = str(job.get("image_alt", "") or job.get("content", "") or "").strip()
    lang = _language_label(job)
    return AITextService().generate_image_prompt_text(
        title=title,
        body=body,
        language=lang,
        provider=_text_provider(job),
        model=_text_model(job),
    )[:1200]


def _regen_image_path(job: dict[str, Any]) -> list[str]:
    pid = str(job.get("page_id", "") or "").strip()
    if not pid:
        raise RuntimeError("Job thiếu page_id — không thể tạo ảnh.")
    title = str(job.get("title", "") or "").strip() or "post"
    body = str(job.get("content", "") or job.get("image_alt", "") or "").strip()
    style = ""
    cfg = job.get("ai_config")
    if isinstance(cfg, dict):
        style = str(cfg.get("image_style", "") or "").strip()
    image_prompt = str(job.get("image_prompt", "") or "").strip()
    stem = f"regen_{uuid.uuid4().hex[:10]}"
    paths = AIImageService().generate_and_save_for_batch(
        page_id=pid,
        file_stem=stem,
        title=title,
        body=body,
        image_style=style,
        image_prompt=image_prompt,
        number_of_images=1,
        provider=_image_provider(job),
        model=_image_model(job),
    )
    return [str(p) for p in paths]


def _merge_image_path_into_media(job: dict[str, Any], new_paths: list[str]) -> list[str]:
    """Thay ảnh đầu tiên trong ``media_files`` (hoặc append), giữ nguyên các file khác."""
    media = [str(p) for p in (job.get("media_files") or []) if str(p).strip()]
    if not new_paths:
        return media
    first_new = new_paths[0]
    for i, p in enumerate(media):
        suf = os.path.splitext(p)[1].lower()
        if suf in IMAGE_SUFFIXES:
            media[i] = first_new
            return media
    media.insert(0, first_new)
    return media


def regenerate_missing_fields_for_job(
    job: dict[str, Any],
    *,
    allowed_fields: Iterable[str] | None = None,
    include_image_generation: bool = True,
) -> tuple[dict[str, Any], list[str]]:
    """
    Chỉ sinh lại các field đang thiếu (hoặc thuộc ``allowed_fields``) trên job.

    Args:
        job: Dict job (không bị mutate — hàm tạo bản sao mới).
        allowed_fields: Nếu ``None`` → sinh toàn bộ field thiếu; nếu được truyền →
            chỉ sinh lại những field trong danh sách này (và chỉ khi chúng đang thiếu).
        include_image_generation: Nếu ``False`` bỏ qua bước tạo ảnh thật (chỉ regen prompt).

    Returns:
        Tuple ``(updated_job, regenerated_fields)``. ``updated_job`` là bản sao patch
        các field mới; ``regenerated_fields`` là danh sách tên field đã thực sự được sinh lại.
    """
    job_id = str(job.get("id", "")).strip() or "(no-id)"
    missing = set(get_missing_fields(job))
    if allowed_fields is not None:
        allowed = {f for f in allowed_fields if f in REGENERABLE_FIELDS}
        # Intersect với field thiếu — không đụng field đã có dữ liệu hợp lệ
        target_set = missing & allowed
    else:
        target_set = set(missing)

    target_set -= PROTECTED_FIELDS  # không regen post_type/page_id/...
    to_do = order_regenerable_fields(target_set)

    if not to_do:
        logger.info("[Regen][{}] Không có field nào cần/được phép sinh lại.", job_id[:8])
        return dict(job), []

    logger.info("[Regen][{}] Bắt đầu sinh lại: {}", job_id[:8], ", ".join(to_do))
    patched = dict(job)
    regenerated: list[str] = []

    for field in to_do:
        try:
            if field == "title":
                new_title = _regen_title(patched)
                if new_title:
                    patched["title"] = new_title
                    regenerated.append("title")
                    logger.info("[Regen][{}] title ← {!r}", job_id[:8], _truncate(new_title, 60))
            elif field == "content":
                body, alt = _regen_content(patched)
                patched["content"] = body
                if alt and not str(patched.get("image_alt", "") or "").strip():
                    patched["image_alt"] = alt
                regenerated.append("content")
                logger.info("[Regen][{}] content ({} ký tự)", job_id[:8], len(body))
            elif field == "hashtags":
                tags = _regen_hashtags(patched)
                if tags:
                    patched["hashtags"] = tags
                    regenerated.append("hashtags")
                    logger.info("[Regen][{}] hashtags ← {}", job_id[:8], ", ".join(tags))
                    if not str(patched.get("cta", "") or "").strip():
                        cta = _regen_cta(patched)
                        if cta:
                            patched["cta"] = cta
                            logger.info("[Regen][{}] cta ← {!r}", job_id[:8], _truncate(cta, 60))
            elif field == "image_prompt":
                prompt = _regen_image_prompt(patched)
                if prompt:
                    patched["image_prompt"] = prompt
                    regenerated.append("image_prompt")
                    logger.info("[Regen][{}] image_prompt ({} ký tự)", job_id[:8], len(prompt))
            elif field == "image_path":
                if not include_image_generation:
                    logger.info("[Regen][{}] Bỏ qua image_path (include_image_generation=False).", job_id[:8])
                    continue
                new_paths = _regen_image_path(patched)
                if new_paths:
                    merged = _merge_image_path_into_media(patched, new_paths)
                    patched["media_files"] = merged
                    patched["job_post_image_path"] = new_paths[0]
                    regenerated.append("image_path")
                    logger.info("[Regen][{}] image_path ← {}", job_id[:8], new_paths[0])
        except Exception as exc:  # noqa: BLE001
            logger.warning("[Regen][{}] Sinh {} lỗi: {}", job_id[:8], field, exc)

    logger.info(
        "[Regen][{}] Hoàn tất. Đã cập nhật: {}",
        job_id[:8],
        ", ".join(regenerated) or "(không có field nào)",
    )
    return patched, regenerated


def regenerate_many_jobs(
    jobs: Iterable[dict[str, Any]],
    *,
    allowed_fields: Iterable[str] | None = None,
    include_image_generation: bool = True,
    on_progress: Any = None,
) -> list[tuple[dict[str, Any], list[str]]]:
    """
    Chạy ``regenerate_missing_fields_for_job`` cho nhiều job (tuần tự).

    ``on_progress(index, total, job_id, regen_fields)`` được gọi mỗi khi một job
    hoàn tất — tiện cập nhật UI/thanh trạng thái.
    """
    items = list(jobs)
    out: list[tuple[dict[str, Any], list[str]]] = []
    total = len(items)
    for i, j in enumerate(items, start=1):
        updated, regen = regenerate_missing_fields_for_job(
            j,
            allowed_fields=allowed_fields,
            include_image_generation=include_image_generation,
        )
        out.append((updated, regen))
        if callable(on_progress):
            try:
                on_progress(i, total, str(j.get("id", "")), regen)
            except Exception:  # noqa: BLE001
                logger.debug("on_progress callback lỗi (bỏ qua).")
    return out
