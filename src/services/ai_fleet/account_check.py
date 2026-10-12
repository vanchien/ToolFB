"""Kiểm tra khóa AI trước khi cho chọn. Không ghi khóa vào log hay thông báo lỗi."""

from __future__ import annotations

from typing import Any

import requests

_KINDS = {"openai": "openai_keys", "gemini": "gemini_keys"}


def check_provider_key(kind: str, key: str, *, get: Any | None = None) -> str:
    """Trả về chuỗi rỗng khi khóa dùng được. Thông báo lỗi không chứa khóa."""
    secret = str(key or "").strip()
    provider = str(kind or "").strip().lower()
    if provider not in _KINDS:
        return "Chỉ kiểm tra tài khoản ChatGPT hoặc Gemini."
    if not secret:
        return "Thiếu khóa."
    caller = get or requests.get
    try:
        if provider == "openai":
            response = caller(
                "https://api.openai.com/v1/models",
                headers={"Authorization": f"Bearer {secret}"},
                timeout=20,
            )
        else:
            response = caller(
                "https://generativelanguage.googleapis.com/v1beta/models",
                params={"key": secret},
                timeout=20,
            )
    except Exception:
        return "Không kết nối được để kiểm tra."
    status = int(getattr(response, "status_code", 0) or 0)
    if status == 200:
        return ""
    return f"Kiểm tra thất bại HTTP {status}."


def existing_key_id(kind: str, key: str) -> str:
    """Tìm id khóa đã lưu. Không trả nội dung khóa."""
    from src.utils.app_secrets import load_normalized_secrets

    secret = str(key or "").strip()
    pool = _KINDS.get(str(kind or "").strip().lower(), "")
    if not secret or not pool:
        return ""
    for row in load_normalized_secrets().get(pool) or []:
        if str(row.get("key") or "") == secret:
            return str(row.get("id") or "")
    return ""


def _chosen_or_verified(store: Any, kind: str) -> list[str]:
    chosen = [str(item) for item in (store.selected_accounts().get(kind) or []) if str(item).strip()]
    if chosen:
        return chosen
    return [row["id"] for row in store.verified_accounts() if row.get("kind") == kind and row.get("id")]


def apply_account_rotation(store: Any, index: int) -> None:
    """Xoay tài khoản đã chọn. Chưa chọn thì dùng tài khoản đã kiểm tra đạt."""
    openai_ids = _chosen_or_verified(store, "openai")
    gemini_ids = _chosen_or_verified(store, "gemini")
    if openai_ids:
        import os

        from src.utils.app_secrets import apply_openai_key_to_environ

        chosen = openai_ids[index % len(openai_ids)]
        os.environ["TOOLFB_CHATGPT_ACCOUNT_ID"] = chosen
        apply_openai_key_to_environ(chosen)
    if gemini_ids:
        from src.utils.app_secrets import apply_gemini_key_to_environ

        apply_gemini_key_to_environ(gemini_ids[index % len(gemini_ids)])
