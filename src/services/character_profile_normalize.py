from __future__ import annotations

"""
Chuẩn hóa / migration nhẹ profile nhân vật auto (snapshot cũ chỉ có reference_image_path,
danh sách ảnh dạng string, thiếu character_id / metadata).
"""

from pathlib import Path
from typing import Any


def normalize_character_image_generations(raw: Any) -> list[dict[str, str]]:
    """
    Đưa ``character_image_generations`` về list dict thống nhất (path + metadata).

    Hỗ trợ legacy: phần tử là string (đường dẫn file).
    """
    if not raw or not isinstance(raw, list):
        return []
    out: list[dict[str, str]] = []
    for x in raw:
        if isinstance(x, str) and str(x).strip():
            p = str(x).strip()
            out.append(
                {
                    "character_image_path": p,
                    "character_image_prompt": "",
                    "image_provider": "",
                    "image_model": "",
                }
            )
        elif isinstance(x, dict):
            p = str(x.get("character_image_path") or x.get("path") or "").strip()
            if not p:
                continue
            out.append(
                {
                    "character_image_path": p,
                    "character_image_prompt": str(x.get("character_image_prompt", "")),
                    "image_provider": str(x.get("image_provider", "")),
                    "image_model": str(x.get("image_model", "")),
                }
            )
    return out


def migrate_auto_character_profile(row: dict[str, Any]) -> dict[str, Any]:
    """
    Bổ sung field mới và đồng bộ ``reference_image_path`` vào danh sách ảnh đã tạo (nếu thiếu).

    Trả về bản copy có thể gán lại vào state / snapshot.
    """
    out = dict(row)
    gens = normalize_character_image_generations(out.get("character_image_generations"))
    ref = str(out.get("reference_image_path", "")).strip()
    if ref:
        paths_in = {g["character_image_path"] for g in gens}
        try:
            ref_resolved = str(Path(ref).resolve())
        except Exception:
            ref_resolved = ref
        aliases = {ref, ref_resolved} if ref_resolved != ref else {ref}
        if not paths_in.intersection(aliases):
            gens.insert(
                0,
                {
                    "character_image_path": ref,
                    "character_image_prompt": str(out.get("character_image_prompt", "")).strip(),
                    "image_provider": str(out.get("image_provider", "")).strip(),
                    "image_model": str(out.get("image_model", "")).strip(),
                },
            )
    out["character_image_generations"] = gens
    if not str(out.get("character_id", "")).strip():
        out["character_id"] = ""
    out.setdefault("character_image_prompt", str(out.get("character_image_prompt", "")).strip())
    out.setdefault("image_provider", str(out.get("image_provider", "")).strip())
    out.setdefault("image_model", str(out.get("image_model", "")).strip())
    return out


def migrate_auto_character_profiles(rows: list[Any] | None) -> list[dict[str, Any]]:
    """Migration hàng loạt cho ``auto_character_profiles`` trong snapshot / builder."""
    if not rows or not isinstance(rows, list):
        return []
    return [migrate_auto_character_profile(dict(x)) for x in rows if isinstance(x, dict)]
