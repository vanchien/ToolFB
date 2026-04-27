"""
Phát hiện field thiếu trên job ``schedule_posts.json`` + filter & trình bày.

- Pure, không phụ thuộc GUI → dễ unit-test.
- Logic "thiếu" tôn trọng ``post_type`` (ví dụ ``text`` không yêu cầu ``image_prompt``).
- Dùng chung bởi bảng job (cột «Thiếu field», bộ lọc) và service regenerate.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, Literal

FieldName = Literal[
    "title",
    "content",
    "hashtags",
    "image_prompt",
    "image_path",
    "video_path",
    "scheduled_at",
    "post_type",
    "page_id",
    "account_id",
    "ai_topic",
    "ai_content_style",
]

# Các field có thể sinh lại bằng AI/helper (theo dependency order).
REGENERABLE_FIELDS: tuple[str, ...] = (
    "title",
    "content",
    "hashtags",
    "image_prompt",
    "image_path",
)

# Các field quan trọng không được sinh lại tự động (chỉ flag cảnh báo).
CRITICAL_FIELDS: tuple[str, ...] = (
    "post_type",
    "page_id",
    "account_id",
    "scheduled_at",
)

# Các field AI metadata (tham chiếu / không bắt buộc).
AI_META_FIELDS: tuple[str, ...] = (
    "ai_topic",
    "ai_content_style",
)

FIELD_LABELS: dict[str, str] = {
    "title": "title",
    "content": "content",
    "hashtags": "hashtags",
    "image_prompt": "image_prompt",
    "image_path": "image_path",
    "video_path": "video_path",
    "scheduled_at": "scheduled_at",
    "post_type": "post_type",
    "page_id": "page_id",
    "account_id": "account_id",
    "ai_topic": "ai_topic",
    "ai_content_style": "ai_content_style",
}

IMAGE_SUFFIXES: frozenset[str] = frozenset({".png", ".jpg", ".jpeg", ".webp", ".gif"})
VIDEO_SUFFIXES: frozenset[str] = frozenset({".mp4", ".mov", ".avi", ".mkv", ".webm"})


def _is_str_blank(v: Any) -> bool:
    if v is None:
        return True
    if not isinstance(v, str):
        return False
    return not v.strip()


def _is_list_empty(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, list):
        return not any(str(x).strip() for x in v)
    if isinstance(v, str):
        return not v.strip()
    return False


def _post_type_needs_image(pt: str) -> bool:
    pt_lc = (pt or "").strip().lower()
    return pt_lc in {"image", "text_image"}


def _post_type_needs_video(pt: str) -> bool:
    pt_lc = (pt or "").strip().lower()
    return pt_lc in {"video", "text_video"}


def _post_type_needs_text_body(pt: str) -> bool:
    """``content`` gần như luôn cần cho các job trừ ``image`` / ``video`` thuần."""
    pt_lc = (pt or "").strip().lower()
    return pt_lc in {"text", "text_image", "text_video"}


def _first_media_of_kind(media_files: Any, suffixes: frozenset[str]) -> str:
    if not isinstance(media_files, list):
        return ""
    for p in media_files:
        s = str(p or "").strip()
        if not s:
            continue
        if Path(s).suffix.lower() in suffixes:
            return s
    return ""


def _file_exists(path: str) -> bool:
    if not path:
        return False
    try:
        return Path(path).is_file()
    except OSError:
        return False


def get_missing_fields(job: dict[str, Any]) -> list[str]:
    """
    Trả về danh sách tên field đang thiếu của một job, tôn trọng ``post_type``.

    Một field bị coi là thiếu nếu:
    - không có key
    - giá trị None / chuỗi rỗng / chuỗi chỉ có khoảng trắng
    - list rỗng với field bắt buộc là list
    - path media không tồn tại thực tế (với ``image_path`` / ``video_path``)
    """
    missing: list[str] = []
    j = dict(job or {})
    pt = str(j.get("post_type", "")).strip().lower()

    if not str(j.get("account_id", "")).strip():
        missing.append("account_id")
    if not str(j.get("page_id", "")).strip():
        missing.append("page_id")
    if not pt:
        missing.append("post_type")
    if not str(j.get("scheduled_at", "")).strip():
        missing.append("scheduled_at")

    if _is_str_blank(j.get("title")):
        missing.append("title")

    if _post_type_needs_text_body(pt) and _is_str_blank(j.get("content")):
        missing.append("content")

    if _is_list_empty(j.get("hashtags")):
        missing.append("hashtags")

    if _post_type_needs_image(pt):
        if _is_str_blank(j.get("image_prompt")):
            missing.append("image_prompt")
        img_path = _first_media_of_kind(j.get("media_files"), IMAGE_SUFFIXES) or str(
            j.get("job_post_image_path") or ""
        ).strip()
        if not img_path or not _file_exists(img_path):
            missing.append("image_path")

    if _post_type_needs_video(pt):
        vid_path = _first_media_of_kind(j.get("media_files"), VIDEO_SUFFIXES)
        if not vid_path or not _file_exists(vid_path):
            missing.append("video_path")

    return missing


def format_missing_fields_for_display(missing: Iterable[str], *, max_chars: int = 60) -> str:
    """
    Trả chuỗi ngắn gọn cho cột «Thiếu field» trong bảng job.

    Ví dụ ``["title", "content"]`` → ``"title, content"``.
    """
    items = [FIELD_LABELS.get(m, m) for m in missing if m]
    if not items:
        return ""
    text = ", ".join(items)
    if len(text) > max_chars:
        text = text[: max_chars - 1] + "…"
    return text


MissingPreset = dict[str, Any]

# Preset cho combobox «Thiếu field» trên bảng job.
# Mỗi preset: label hiển thị, danh sách ``fields``, ``match_mode``.
# - ``match_mode='none'`` → không lọc.
# - ``match_mode='any'``  → thiếu ít nhất một trong ``fields``.
# - ``match_mode='all'``  → thiếu đầy đủ toàn bộ ``fields``.
MISSING_FIELD_PRESETS: tuple[MissingPreset, ...] = (
    {"label": "Không lọc", "fields": [], "match_mode": "none"},
    {"label": "Thiếu title", "fields": ["title"], "match_mode": "any"},
    {"label": "Thiếu content", "fields": ["content"], "match_mode": "any"},
    {"label": "Thiếu hashtags", "fields": ["hashtags"], "match_mode": "any"},
    {"label": "Thiếu image prompt", "fields": ["image_prompt"], "match_mode": "any"},
    {"label": "Thiếu image path", "fields": ["image_path"], "match_mode": "any"},
    {"label": "Thiếu video path", "fields": ["video_path"], "match_mode": "any"},
    {"label": "Thiếu title + content", "fields": ["title", "content"], "match_mode": "all"},
    {"label": "Thiếu title + hashtags", "fields": ["title", "hashtags"], "match_mode": "all"},
    {"label": "Thiếu content + hashtags", "fields": ["content", "hashtags"], "match_mode": "all"},
    {
        "label": "Thiếu bất kỳ field AI nào",
        "fields": ["title", "content", "hashtags", "image_prompt"],
        "match_mode": "any",
    },
    {
        "label": "Thiếu bất kỳ field bắt buộc nào",
        "fields": list(REGENERABLE_FIELDS) + list(CRITICAL_FIELDS),
        "match_mode": "any",
    },
)

MISSING_FIELD_LABELS: tuple[str, ...] = tuple(p["label"] for p in MISSING_FIELD_PRESETS)


def preset_by_label(label: str) -> MissingPreset:
    """Tra preset theo nhãn hiển thị. Fallback về «Không lọc»."""
    for p in MISSING_FIELD_PRESETS:
        if p["label"] == label:
            return p
    return MISSING_FIELD_PRESETS[0]


def filter_jobs_by_missing_fields(
    jobs: Iterable[dict[str, Any]],
    selected_fields: Iterable[str],
    *,
    match_mode: str = "any",
) -> list[dict[str, Any]]:
    """
    Lọc danh sách job theo các field đang thiếu.

    Args:
        jobs: Danh sách job gốc (dict).
        selected_fields: Field cần lọc (ví dụ ``["title", "content"]``). Rỗng = không lọc.
        match_mode:
            - ``any``: thiếu ít nhất một field trong ``selected_fields``.
            - ``all``: thiếu đủ toàn bộ field trong ``selected_fields``.
            - ``none``: không lọc (trả về toàn bộ).

    Returns:
        Danh sách job khớp điều kiện.
    """
    sel = {str(f).strip() for f in selected_fields if str(f).strip()}
    mode = (match_mode or "any").strip().lower()
    if mode == "none" or not sel:
        return list(jobs)
    out: list[dict[str, Any]] = []
    for j in jobs:
        miss = set(get_missing_fields(j))
        if mode == "all":
            if sel.issubset(miss):
                out.append(j)
        else:
            if miss & sel:
                out.append(j)
    return out


def order_regenerable_fields(fields: Iterable[str]) -> list[str]:
    """
    Sắp xếp lại danh sách field theo thứ tự dependency để sinh đúng thứ tự:
    title → content → hashtags → image_prompt → image_path.
    Field không thuộc ``REGENERABLE_FIELDS`` bị loại bỏ.
    """
    s = set(str(f).strip() for f in fields if str(f).strip())
    return [f for f in REGENERABLE_FIELDS if f in s]
