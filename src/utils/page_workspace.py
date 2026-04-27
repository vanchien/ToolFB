"""
Thư mục làm việc theo Page: thư viện nội dung + ``page_ai_config.json``.

Đường dẫn gốc: ``data/pages/<page_id>/`` (``page_id`` an toàn, không chứa ``..``).
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from loguru import logger

from src.utils.paths import project_root

_PAGE_ID_SAFE = re.compile(r"^[a-zA-Z0-9_-]{1,64}$")


def sanitize_page_id(page_id: str) -> str:
    s = str(page_id).strip()
    if not s or ".." in s or "/" in s or "\\" in s:
        raise ValueError("page_id không hợp lệ.")
    if not _PAGE_ID_SAFE.match(s):
        raise ValueError("page_id chỉ gồm chữ, số, gạch ngang/gạch dưới (tối đa 64 ký tự).")
    return s


def page_workspace_root(page_id: str) -> Path:
    """``data/pages/<page_id>/`` (đã chuẩn hóa id)."""
    sid = sanitize_page_id(page_id)
    return project_root() / "data" / "pages" / sid


def default_page_ai_config(page_id: str) -> dict[str, Any]:
    """Mặc định theo PRD (mục 13.2), có thể mở rộng trường."""
    return {
        "page_id": sanitize_page_id(page_id),
        "brand_voice": "",
        "content_pillars": [],
        "target_audience": "",
        "post_length": "medium",
        "emoji_style": "light",
        "cta_style": "soft",
        "hashtags": [],
        "image_style": "",
        "avoid_keywords": [],
        "auto_generate_image": False,
        "auto_generate_caption": False,
    }


def page_ai_config_path(page_id: str) -> Path:
    return page_workspace_root(page_id) / "page_ai_config.json"


def ensure_page_workspace(page_id: str) -> Path:
    """
    Tạo cây thư mục thư viện + ``page_ai_config.json`` nếu chưa có.

    Returns:
        Thư mục gốc workspace của Page.
    """
    root = page_workspace_root(page_id)
    subdirs = (
        root / "library" / "texts",
        root / "library" / "images",
        root / "library" / "videos",
        root / "library" / "drafts",
        root / "library" / "generated",
        root / "prompts",
        root / "history",
    )
    for d in subdirs:
        d.mkdir(parents=True, exist_ok=True)
    cfg_path = root / "page_ai_config.json"
    if not cfg_path.is_file():
        cfg = default_page_ai_config(page_id)
        cfg_path.write_text(json.dumps(cfg, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        logger.info("Đã tạo page_ai_config.json: {}", cfg_path)
    logger.debug("Đã đảm bảo workspace Page: {}", root)
    return root


def load_page_ai_config(page_id: str) -> dict[str, Any]:
    """Đọc + merge với mặc định (thiếu key → bổ sung)."""
    ensure_page_workspace(page_id)
    path = page_ai_config_path(page_id)
    base = default_page_ai_config(page_id)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return dict(base)
    if not isinstance(raw, dict):
        return dict(base)
    out = dict(base)
    out.update(raw)
    out["page_id"] = base["page_id"]
    return out


def save_page_ai_config(page_id: str, data: dict[str, Any]) -> None:
    """Ghi ``page_ai_config.json`` — merge lên bản hiện có + mặc định."""
    sid = sanitize_page_id(page_id)
    ensure_page_workspace(sid)
    merged = load_page_ai_config(sid)
    for k, v in data.items():
        if k == "page_id":
            continue
        merged[k] = v
    merged["page_id"] = sid
    path = page_ai_config_path(sid)
    path.write_text(json.dumps(merged, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    logger.info("Đã lưu page_ai_config.json page_id={}", sid)
