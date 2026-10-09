"""Chống đăng trùng cùng target, group, URL và nội dung."""

from __future__ import annotations

import hashlib
from pathlib import Path
from urllib.parse import urlparse

_IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".gif"}
_VIDEO_EXTENSIONS = {".mp4", ".mov", ".avi", ".mkv", ".webm"}


SOURCE_POST = "FACEBOOK_POST_URL"
SOURCE_VIDEO = "FACEBOOK_VIDEO_URL"
SOURCE_PUBLIC = "SUPPORTED_PUBLIC_URL"


def text_hash(text: str) -> str:
    raw = " ".join((text or "").split())
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def share_fingerprint(
    target_id: str,
    group_id: str,
    source_url: str,
    text: str,
    images: list[str] | None = None,
) -> str:
    url = " ".join((source_url or "").split())
    names = ",".join(Path(path).name for path in (images or []) if str(path).strip())
    blob = text if not names else f"{text}\n{names}"
    return "|".join((str(target_id or "").strip(), str(group_id or "").strip(), url, text_hash(blob)))


def normalize_share_images(paths: list[str] | None) -> list[str]:
    """Ảnh người dùng chọn trên máy. Không nhận video và không tải file từ link."""
    cleaned: list[str] = []
    for raw in paths or []:
        path = str(raw or "").strip()
        if not path:
            continue
        suffix = Path(path).suffix.casefold()
        if suffix in _VIDEO_EXTENSIONS:
            raise ValueError("Không gửi video. Chỉ ảnh người dùng chọn và link bài.")
        if suffix not in _IMAGE_EXTENSIONS:
            raise ValueError("Ảnh phải là jpg, png, webp hoặc gif")
        cleaned.append(path)
    return cleaned


def classify_source_url(url: str) -> str:
    """Phân loại URL nguồn. Không tải file."""
    parsed = urlparse((url or "").strip())
    host = (parsed.netloc or "").casefold()
    path = (parsed.path or "").casefold()
    if "facebook.com" in host or host.endswith("fb.watch"):
        if any(token in path for token in ("/videos", "/reel", "/watch")) or "watch" in (parsed.query or "").casefold():
            return SOURCE_VIDEO
        return SOURCE_POST
    return SOURCE_PUBLIC


def link_preview(url: str, html: str = "") -> dict[str, str]:
    """Đọc tiêu đề nếu HTML đã có. Không tải video hay ảnh."""
    import re

    title = ""
    description = ""
    match = re.search(r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)', html or "", re.I)
    if not match:
        match = re.search(r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:title["\']', html or "", re.I)
    if match:
        title = " ".join(match.group(1).split())
    desc = re.search(r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\']([^"\']+)', html or "", re.I)
    if desc:
        description = " ".join(desc.group(1).split())
    return {
        "title": title,
        "description": description,
        "thumbnail": "",
        "domain": urlparse(url or "").netloc,
        "source_url": (url or "").strip(),
    }


def assert_link_only(payload: dict) -> None:
    """Publisher chỉ nhận chữ và URL."""
    if payload.get("video_path") or payload.get("download") or payload.get("upload"):
        raise ValueError("Publisher không tải hoặc upload video")
    if not str(payload.get("source_url") or "").strip():
        raise ValueError("Thiếu URL nguồn")
