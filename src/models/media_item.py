"""Metadata file import vào thư viện Page (``*.import.meta.json``)."""

from __future__ import annotations

from typing import Literal, TypedDict

MediaKind = Literal["text", "image", "video"]


class MediaImportMeta(TypedDict, total=False):
    """Ghi cạnh file sau khi ``LibraryService.import_file``."""

    kind: MediaKind
    page_id: str
    source_path: str
    stored_path: str
    original_name: str
    imported_at: str
    size_bytes: int
