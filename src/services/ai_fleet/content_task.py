"""Tạo bản thảo từ đúng một provider. Lỗi thì dừng, không gọi nhà cung cấp khác."""

from __future__ import annotations

from typing import Any

from src.services.ai_fleet.store import FleetStore


def draft_from_provider(
    store: FleetStore,
    *,
    page_id: str,
    topic: str,
    provider: Any,
    model: str | None = None,
    source_urls: list[str] | None = None,
    reject_duplicate: bool = False,
) -> dict[str, Any]:
    """Viết một bài cho đúng Page rồi để ở trạng thái chờ duyệt. Không đưa vào lịch."""
    name = str(getattr(provider, "provider_name", "") or "").strip().lower()
    if name not in {"openai", "gemini"}:
        raise RuntimeError("Chỉ dùng provider văn bản đã cấu hình.")
    profile = (store.load().get("profiles") or {}).get(page_id)
    if not profile:
        raise RuntimeError("Chưa có hồ sơ Page.")
    if str(profile.get("status") or "").upper() == "PAUSED":
        raise RuntimeError("Page Agent đang tạm dừng.")
    language = str(profile.get("language") or "").strip()
    voice = str(profile.get("brand_voice") or "").strip()
    prompt = (
        "Trả về một JSON object với các khóa title, body, caption, hashtags, image_prompt, language. "
        f"Ngôn ngữ: {language or 'theo hồ sơ Page'}. Giọng: {voice or 'tự nhiên'}. "
        f"Chủ đề: {str(topic or '').strip()}. Không bịa số liệu. Nếu không có nguồn thì để source trống."
        f"{_master_orders(store)}"
    )
    package = provider.generate_content_package(prompt=prompt, model=model)
    if str(package.get("provider") or name) != name:
        raise RuntimeError("Kết quả không đúng provider đã chọn.")
    if reject_duplicate and _same_text_exists(store, page_id, package):
        return {"content_id": "", "page_id": page_id, "status": "SKIPPED_DUPLICATE", "provider": name}
    fields = dict(package)
    fields["source_urls"] = [item.strip() for item in (source_urls or fields.get("source_urls") or []) if str(item).strip()]
    fields["hashtags"] = _hashtags(fields.get("hashtags"))
    fields.setdefault("asset_ids", [])
    fields.setdefault("score", 0)
    if language:
        fields["language"] = language
    content = store.create_draft(page_id, fields)
    if str(content.get("status") or "") == "APPROVED":
        raise RuntimeError("Bản thảo mới không được tự duyệt.")
    return content


def _master_orders(store: FleetStore) -> str:
    """Lệnh vừa ghi được đọc lại từ file, nên vòng đang chạy nhận ngay."""
    doc = store.load()
    current = str(doc.get("master_instruction") or "").strip()
    learned: list[str] = []
    for row in (doc.get("master_lessons") or [])[-5:]:
        if not isinstance(row, dict):
            continue
        text = str(row.get("text") or "").strip()
        if text and text != current and text not in learned:
            learned.append(text)
    parts = []
    if current:
        parts.append(f"Lệnh agent tổng đang áp dụng: {current}.")
    if learned:
        parts.append("Các lệnh đã học: " + " | ".join(learned) + ".")
    return (" " + " ".join(parts)) if parts else ""


def _hashtags(raw: object) -> list[str]:
    if isinstance(raw, str):
        parts = raw.replace(",", " ").split()
    else:
        parts = list(raw or [])
    return [str(item).strip() for item in parts if str(item).strip()]


def _same_text_exists(store: FleetStore, page_id: str, package: dict[str, Any]) -> bool:
    """Trùng đúng tiêu đề, thân bài, caption và hashtag của một bài khác trên Page."""
    from src.services.ai_fleet.gate import content_hash

    policy = store.current_policy(page_id)
    if policy is None:
        return False
    digest = content_hash(
        {
            "title": package.get("title"),
            "body": package.get("body"),
            "caption": package.get("caption"),
            "hashtags": package.get("hashtags") or [],
            "asset_ids": [],
            "page_id": page_id,
            "policy_version": int(policy.get("version") or 0),
        }
    )
    return digest in store.other_hashes(page_id, "")
