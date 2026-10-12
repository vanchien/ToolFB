"""Viết bài và tạo ảnh bằng hai nhà cung cấp độc lập. Không đổi nhà khi một phần lỗi."""

from __future__ import annotations

import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from src.services.ai_fleet.account_check import apply_account_rotation
from src.services.ai_fleet.content_task import draft_from_provider
from src.services.ai_fleet.store import FleetStore
from src.services.ai_fleet.tasks import enqueue, fail_task, note_progress, transition
from src.services.ai_provider_factory import AIProviderFactory
from src.utils.media_library import save_upload_to_library

TEXT_PROVIDERS = ("openai", "gemini")
IMAGE_PROVIDERS = ("openai", "gemini", "nanobanana", "nano_banana_pro")


def text_provider(name: str) -> Any:
    """Nhà viết bài. Chỉ nhận provider có gói JSON. Không tự chuyển sang nhà khác."""
    chosen = str(name or "").strip().lower()
    if chosen not in TEXT_PROVIDERS:
        raise RuntimeError("Nhà viết bài này chưa có gói bài có cấu trúc. Hãy chọn ChatGPT hoặc Gemini.")
    provider = AIProviderFactory.text(chosen)
    if not hasattr(provider, "generate_content_package"):
        raise RuntimeError("Nhà viết bài chưa trả đúng cấu trúc title, body, caption, hashtags.")
    return provider


def create_post(
    store: FleetStore,
    *,
    page_id: str,
    topic: str,
    text_name: str,
    image_name: str,
    source_urls: list[str] | None = None,
    make_image: bool = False,
) -> dict[str, Any]:
    """Tạo một bản thảo. Ảnh chỉ gọi đúng nhà đã chọn và gắn vào bài này."""
    if store.load().get("emergency_stop"):
        raise RuntimeError("Đã dừng khẩn cấp. Không tạo tác vụ mới.")
    task = enqueue(store, kind="write", page_id=page_id, detail=str(topic or "")[:120])
    task["text_name"] = text_name
    task["image_name"] = image_name
    task["make_image"] = bool(make_image)
    store.save_task(task)
    transition(store, task["task_id"], "running", note=f"Viết bằng {text_name}, ảnh bằng {image_name}")
    try:
        result = _create_post_body(
            store,
            page_id=page_id,
            topic=topic,
            text_name=text_name,
            image_name=image_name,
            source_urls=source_urls,
            make_image=make_image,
            task_id=task["task_id"],
        )
    except Exception as exc:
        fail_task(store, task["task_id"], exc)
        raise
    content = result.get("content") or {}
    if result.get("skipped"):
        transition(store, task["task_id"], "succeeded", note=f"Bỏ qua: {result['skipped']}", detail=str(result["skipped"]))
    elif not content and result.get("image_error"):
        transition(store, task["task_id"], "failed", note=str(result["image_error"]))
    else:
        transition(
            store,
            task["task_id"],
            "waiting_approval",
            note="Bản thảo chờ duyệt",
            content_id=str(content.get("content_id") or ""),
        )
    return result


def resume_task(store: FleetStore, task_id: str) -> dict[str, Any]:
    """Chạy lại đúng tác vụ đang ở hàng chờ. Không tạo tác vụ mới và không đổi nhà cung cấp."""
    if store.load().get("emergency_stop"):
        raise RuntimeError("Đã dừng khẩn cấp. Không chạy lại tác vụ.")
    task = next((row for row in store.tasks() if str(row.get("task_id") or "") == task_id), None)
    if task is None or str(task.get("state") or "") != "queued":
        raise ValueError("Chỉ chạy tác vụ đang ở hàng chờ.")
    transition(store, task_id, "running", note="Chạy lại")
    try:
        result = _create_post_body(
            store,
            page_id=str(task.get("page_id") or ""),
            topic=str(task.get("detail") or ""),
            text_name=str(task.get("text_name") or ""),
            image_name=str(task.get("image_name") or ""),
            make_image=bool(task.get("make_image")),
            task_id=task_id,
        )
    except Exception as exc:
        fail_task(store, task_id, exc)
        raise
    content = result.get("content") or {}
    if result.get("skipped"):
        transition(store, task_id, "succeeded", note=f"Bỏ qua: {result['skipped']}")
    elif not content and result.get("image_error"):
        transition(store, task_id, "failed", note=str(result["image_error"]))
    else:
        transition(store, task_id, "waiting_approval", note="Bản thảo chờ duyệt", content_id=str(content.get("content_id") or ""))
    return result


def _create_post_body(
    store: FleetStore,
    *,
    page_id: str,
    topic: str,
    text_name: str,
    image_name: str,
    source_urls: list[str] | None = None,
    make_image: bool = False,
    task_id: str = "",
) -> dict[str, Any]:
    """Phần gọi nhà cung cấp. Bản thảo đã có thì không gọi nhà viết bài lần nữa."""
    existing = _existing_draft(store, task_id)
    if existing is not None:
        content = existing
    else:
        content = _draft(
            store,
            page_id=page_id,
            topic=topic,
            text_name=text_name,
            source_urls=source_urls,
        )
        if task_id and str(content.get("content_id") or ""):
            note_progress(store, task_id, content_id=str(content["content_id"]))
    if str(content.get("status") or "") == "SKIPPED_DUPLICATE":
        return {"skipped": "duplicate", "content": None, "asset_id": ""}
    version = next(iter(content.get("versions") or []), {})
    asset_id = str((version.get("asset_ids") or [""])[0] or "")
    image_error = ""
    if make_image and not asset_id:
        try:
            asset_id = _attach_image(store, content, image_name=image_name)
            _add_usage(store, content["content_id"], image_calls=1)
        except Exception as exc:  # noqa: BLE001
            image_error = _safe_error(exc)
    if make_image and not asset_id:
        content_id = str(content.get("content_id") or "")
        if content_id:
            try:
                store.delete_content(content_id)
            except Exception:
                pass
        return {
            "skipped": "",
            "content": None,
            "asset_id": "",
            "image_error": image_error or "Chưa tạo được ảnh.",
            "text_provider": text_name,
        }
    return {
        "skipped": "",
        "content": store.content(content["content_id"]),
        "asset_id": asset_id,
        "image_error": image_error,
        "text_provider": text_name,
    }


def _draft(
    store: FleetStore,
    *,
    page_id: str,
    topic: str,
    text_name: str,
    source_urls: list[str] | None,
) -> dict[str, Any]:
    return draft_from_provider(
        store,
        page_id=page_id,
        topic=topic,
        provider=text_provider(text_name),
        source_urls=source_urls,
        reject_duplicate=True,
    )


def _enable_saved_gemini() -> bool:
    """Bật khóa Gemini đã lưu. Không in khóa."""
    try:
        from src.utils.app_secrets import apply_gemini_key_to_environ

        return bool(apply_gemini_key_to_environ(None) or os.environ.get("GEMINI_API_KEY", "").strip())
    except Exception:
        return bool(os.environ.get("GEMINI_API_KEY", "").strip())


def _existing_draft(store: FleetStore, task_id: str) -> dict[str, Any] | None:
    if not task_id:
        return None
    task = next((row for row in store.tasks() if str(row.get("task_id") or "") == task_id), None)
    content_id = str((task or {}).get("content_id") or "")
    if not content_id:
        return None
    content = store.content(content_id)
    if not content or str(content.get("status") or "") in {"PUBLISHED", "REJECTED"}:
        return None
    return content


def create_batch(
    store: FleetStore,
    *,
    page_id: str,
    topics: list[str],
    text_name: str,
    image_name: str,
    make_image: bool = False,
    source_urls: list[str] | None = None,
    max_items: int = 5,
    max_output_tokens: int = 0,
) -> dict[str, Any]:
    """Viết lần lượt. Bỏ bài trùng. Dừng khi đủ số bài hoặc vượt ngân sách token đầu ra."""
    limit = max(1, min(10, int(max_items or 1)))
    budget = max(0, int(max_output_tokens or 0))
    spent = 0
    rows: list[dict[str, Any]] = []
    seen_topics: set[str] = set()
    for topic in [str(item).strip() for item in topics if str(item).strip()]:
        key = topic.casefold()
        if key in seen_topics:
            rows.append({"topic": topic, "skipped": "duplicate", "content": None})
            continue
        seen_topics.add(key)
        if len([row for row in rows if row.get("content")]) >= limit:
            rows.append({"topic": topic, "skipped": "limit", "content": None})
            continue
        if budget and spent >= budget:
            rows.append({"topic": topic, "skipped": "budget", "content": None})
            continue
        try:
            result = create_post(
                store,
                page_id=page_id,
                topic=topic,
                text_name=text_name,
                image_name=image_name,
                make_image=make_image,
                source_urls=source_urls,
            )
        except Exception as exc:  # noqa: BLE001
            rows.append({"topic": topic, "skipped": "error", "content": None, "error": _safe_error(exc)})
            break
        content = result.get("content") or {}
        spent += int((content.get("usage") or {}).get("output_tokens") or 0)
        rows.append({"topic": topic, **result})
    return {"items": rows, "output_tokens": spent}


def accept_if_clear(store: FleetStore, content_id: str, *, require_image: bool = False) -> str:
    """Duyệt tự động khi mọi kiểm tra cứng đạt. Bài có ảnh thì agent tổng mới được duyệt."""
    from src.services.ai_fleet.gate import hard_checks, review_content, schedule_block_reason

    content = store.content(content_id)
    version = store.current_version(content_id)
    if not content or version is None:
        return "Không thấy nội dung."
    policy = store.current_policy(str(content.get("page_id") or ""))
    if policy is None:
        return "Page chưa có policy."
    check_policy = dict(policy)
    if require_image:
        check_policy["requires_media"] = True
    checks = hard_checks(version, check_policy, known_hashes=store.other_hashes(str(content["page_id"]), content_id))
    score = int(policy.get("min_score") or 80) if not any(row.get("status") == "FAIL" for row in checks) else 0
    review = review_content(store, content_id, score=score, require_image=require_image)
    note_review(store, content_id, review)
    if require_image and not (version.get("asset_ids") or []):
        return "Agent tổng chưa duyệt vì bài chưa có ảnh."
    return schedule_block_reason(content_id, store)


def save_post_edit(
    store: FleetStore,
    content_id: str,
    *,
    title: str,
    body: str,
    caption: str,
    hashtags: list[str] | str,
) -> dict[str, Any]:
    """Lưu bản người dùng sửa. Bài đã đăng hoặc đã từ chối thì giữ nguyên."""
    content = store.content(content_id)
    if not content:
        raise ValueError("Không thấy bài.")
    if content.get("published") or str(content.get("status") or "") in {"PUBLISHED", "REJECTED"}:
        raise ValueError("Bài đã đăng hoặc đã từ chối thì không sửa.")
    version = store.current_version(content_id) or {}
    tags = _tag_list(hashtags)
    fields = {
        "title": str(title or "").strip(),
        "body": str(body or "").strip(),
        "caption": str(caption or "").strip(),
        "hashtags": tags,
        "source_urls": list(version.get("source_urls") or []),
        "asset_ids": list(version.get("asset_ids") or []),
        "language": str(version.get("language") or ""),
        "sensitive": bool(version.get("sensitive")),
        "image_prompt": str(version.get("image_prompt") or ""),
        "provider": str(version.get("provider") or ""),
        "model": str(version.get("model") or ""),
    }
    unchanged = (
        fields["title"] == str(version.get("title") or "").strip()
        and fields["body"] == str(version.get("body") or "").strip()
        and fields["caption"] == str(version.get("caption") or "").strip()
        and tags == [str(item).strip() for item in (version.get("hashtags") or []) if str(item).strip()]
    )
    if unchanged:
        return {"changed": False, "content": content}
    if not fields["caption"] and not fields["body"]:
        raise ValueError("Bài cần có caption hoặc nội dung.")
    return {"changed": True, "content": store.add_version(content_id, fields)}


def place_approved_post(
    store: FleetStore,
    content_id: str,
    *,
    account_id: str,
    posts: Any,
    when: datetime | None = None,
) -> str:
    """Xếp bài đã duyệt. Không truyền giờ thì giữ lịch sẵn có. Truyền giờ thì đăng vào giờ đó."""
    from src.services.ai_fleet.gate import schedule_block_reason

    reason = schedule_block_reason(content_id, store)
    if reason:
        return reason
    if not str(account_id or "").strip():
        return "Page chưa gắn tài khoản."
    content = store.content(content_id) or {}
    version = store.current_version(content_id) or {}
    waiting = [
        job
        for job in _job_rows(posts)
        if str(job.get("fleet_content_id") or "") == content_id
        and str(job.get("status") or "").lower() in {"pending", "ready_queue", "running"}
    ]
    if any(str(job.get("status") or "").lower() == "running" for job in waiting):
        return "Bài đang được đăng."
    pending = [job for job in waiting if str(job.get("status") or "").lower() != "running"]
    if when is None and pending:
        return ""
    target = when or _next_schedule_time(store, posts, str(content.get("page_id") or ""))
    row = dict(pending[0]) if pending else {}
    row.update(
        {
            "page_id": str(content.get("page_id") or ""),
            "account_id": account_id,
            "fleet_content_id": content_id,
            "title": str(version.get("title") or ""),
            "content": str(version.get("caption") or version.get("body") or ""),
            "post_type": "text",
            "status": "pending",
            "scheduled_at": _as_utc_iso(target),
            "job_post_image_path": str(row.get("job_post_image_path") or _asset_file(version)),
        }
    )
    posts.upsert(row)
    return ""


def _next_schedule_time(store: FleetStore, posts: Any, page_id: str) -> datetime:
    """Giờ đăng còn trống của ngày đã viết trước. Hết chỗ thì sang ngày kế tiếp."""
    now = datetime.now(_local_tz())
    profile = (store.load().get("profiles") or {}).get(page_id) or {}
    day = now.date() + timedelta(days=_write_ahead_days())
    for _ in range(3):
        slots = _posting_times(profile, day, now.tzinfo)
        used = {(item.hour, item.minute) for item in _jobs_on_day(posts, page_id, day)}
        for slot in slots:
            if (slot.hour, slot.minute) not in used:
                return slot
        day = day + timedelta(days=1)
    return datetime.now(timezone.utc)


def _asset_file(version: dict[str, Any]) -> str:
    from src.utils.media_library import media_library_dir

    for asset_id in version.get("asset_ids") or []:
        path = media_library_dir() / str(asset_id)
        if path.is_file():
            return str(path)
    return ""


def sync_approved_job(store: FleetStore, content_id: str, posts: Any) -> int:
    """Sau khi duyệt lại, cập nhật chữ job chưa chạy và giữ nguyên giờ đăng."""
    from src.services.ai_fleet.gate import schedule_block_reason

    if schedule_block_reason(content_id, store):
        return 0
    version = store.current_version(content_id) or {}
    text = str(version.get("caption") or version.get("body") or "").strip()
    title = str(version.get("title") or "").strip()
    updated = 0
    for job in _job_rows(posts):
        if str(job.get("fleet_content_id") or "") != content_id:
            continue
        if str(job.get("status") or "").lower() not in {"pending", "ready_queue"}:
            continue
        row = dict(job)
        row["title"] = title
        row["content"] = text
        image_path = _asset_file(version)
        if image_path:
            row["job_post_image_path"] = image_path
        posts.upsert(row)
        updated += 1
    return updated


def _tag_list(hashtags: list[str] | str) -> list[str]:
    if isinstance(hashtags, str):
        parts = hashtags.replace(",", " ").split()
    else:
        parts = [str(item) for item in hashtags]
    return [item.strip() for item in parts if item.strip()]


def queue_if_clear(
    store: FleetStore,
    content_id: str,
    *,
    account_id: str,
    posts: Any,
    scheduled_at: datetime | None = None,
    require_image: bool = False,
) -> str:
    """Chỉ tạo job lịch khi cổng duyệt trả về rỗng. Giờ đăng là giờ đã viết sẵn."""
    reason = accept_if_clear(store, content_id, require_image=require_image)
    if reason:
        if require_image and not _asset_file(store.current_version(content_id) or {}):
            try:
                store.delete_content(content_id)
            except Exception:
                pass
        return reason
    if not str(account_id or "").strip():
        return "Page chưa gắn tài khoản."
    content = store.content(content_id) or {}
    version = store.current_version(content_id) or {}
    asset = ""
    for asset_id in version.get("asset_ids") or []:
        asset = str(asset_id)
        break
    from src.utils.media_library import media_library_dir

    image_path = ""
    if asset and (media_library_dir() / asset).is_file():
        image_path = str(media_library_dir() / asset)
    posts.upsert(
        {
            "page_id": str(content.get("page_id") or ""),
            "account_id": account_id,
            "fleet_content_id": content_id,
            "title": str(version.get("title") or ""),
            "content": str(version.get("caption") or ""),
            "post_type": "text",
            "status": "pending",
            "scheduled_at": _as_utc_iso(scheduled_at),
            "job_post_image_path": image_path,
        }
    )
    return ""


def run_automatic(
    store: FleetStore,
    *,
    pages: list[dict[str, str]],
    text_name: str,
    image_name: str,
    make_image: bool,
    posts: Any,
    max_output_tokens: int = 0,
    max_posts: int | None = None,
    slot_times: list[datetime] | None = None,
    account_index: int = 0,
) -> list[dict[str, str]]:
    """Với mỗi Page đang chạy: viết đủ bài cho ngày hôm sau, duyệt, và xếp đúng giờ đăng."""
    rows: list[dict[str, str]] = []
    spent = 0
    budget = max(0, int(max_output_tokens or 0))
    now = datetime.now(_local_tz())
    for offset, page in enumerate(pages):
        from src.services.ai_fleet.account_check import _chosen_or_verified

        key_count = max(1, len(_chosen_or_verified(store, "openai")))
        text_name = "openai"
        image_name = "openai"
        apply_account_rotation(store, account_index + offset)
        page_id = str(page.get("page_id") or "")
        profile = (store.load().get("profiles") or {}).get(page_id) or {}
        if str(profile.get("status") or "").upper() != "ACTIVE":
            rows.append({"page_id": page_id, "result": "bỏ qua vì Page chưa lưu hoặc đang tạm dừng"})
            continue
        if not str(page.get("account_id") or "").strip():
            rows.append({"page_id": page_id, "result": "Page chưa gắn tài khoản."})
            continue
        topics = [str(item).strip() for item in (profile.get("topic") or []) if str(item).strip()]
        if not topics:
            topics = [str(page.get("page_name") or page_id)]
        slots = list(slot_times) if slot_times is not None else _slots_needed(store, posts, profile, page_id, now)
        if max_posts is not None:
            slots = slots[: max(0, int(max_posts))]
        if not slots:
            rows.append({"page_id": page_id, "result": "đã đủ bài cho ngày sắp đăng"})
            continue
        stop_all = False
        for slot_index, when in enumerate(slots):
            topic = topics[slot_index % len(topics)]
            if budget and spent >= budget:
                rows.append({"page_id": page_id, "result": "dừng vì hết ngân sách token"})
                return rows
            created = None
            for attempt in range(key_count):
                if attempt:
                    apply_account_rotation(store, account_index + offset + attempt)
                try:
                    created = create_post(
                        store,
                        page_id=page_id,
                        topic=topic,
                        text_name=text_name,
                        image_name=image_name,
                        make_image=make_image,
                    )
                except Exception as exc:  # noqa: BLE001
                    message = _safe_error(exc)
                    if "hết credit" in message and attempt + 1 < key_count:
                        continue
                    rows.append({"page_id": page_id, "result": message})
                    from src.services.ai_fleet.tasks import classify_error

                    kind = classify_error(message)
                    if kind == "rate_limit" or (kind == "authentication" and key_count == 1):
                        stop_all = True
                    created = None
                    break
                if make_image and not ((store.current_version(str((created.get("content") or {}).get("content_id") or "")) or {}).get("asset_ids")):
                    missing = str(created.get("image_error") or "Chưa tạo được ảnh.")
                    content_id = str((created.get("content") or {}).get("content_id") or "")
                    if content_id:
                        store.delete_content(content_id)
                    if "hết credit" in missing and attempt + 1 < key_count:
                        created = None
                        continue
                    rows.append({"page_id": page_id, "result": f"Agent tổng chưa duyệt vì bài chưa có ảnh. {missing}"})
                    created = None
                    break
                break
            if created is None:
                if stop_all:
                    return rows
                continue
            content = created.get("content") or {}
            spent += int((content.get("usage") or {}).get("output_tokens") or 0)
            if not content:
                rows.append({"page_id": page_id, "result": f"bỏ qua {created.get('skipped') or 'trùng'}"})
                continue
            reason = queue_if_clear(
                store,
                str(content["content_id"]),
                account_id=str(page.get("account_id") or ""),
                posts=posts,
                scheduled_at=when,
                require_image=make_image,
            )
            note = "đã vào lịch kèm ảnh" if make_image else "đã vào lịch"
            if not reason and created.get("image_error"):
                note = f"{note}. {created['image_error']}"
            rows.append({"page_id": page_id, "result": note if not reason else reason})
        if stop_all:
            return rows
    return rows


def replenish_fleet_posts(
    store: FleetStore | None = None,
    pages: list[dict[str, str]] | None = None,
    posts: Any = None,
    batch: int | None = None,
) -> list[dict[str, str]]:
    """Viết trước một ngày cho Page đang chạy. Tới giờ đã xếp thì lịch hiện có mới đăng."""
    holder = store or FleetStore()
    if holder.load().get("emergency_stop") or not holder.auto_enabled():
        return []
    if posts is None:
        from src.utils.schedule_posts_manager import get_default_schedule_posts_manager

        posts = get_default_schedule_posts_manager()
    discard_posts_without_images(holder, posts)
    if pages is None:
        pages = _live_pages()
    size = batch if batch is not None else int(os.environ.get("FLEET_REPLENISH_BATCH", "3") or "3")
    size = max(1, min(10, int(size)))
    now = datetime.now(_local_tz())
    rows: list[dict[str, str]] = []
    taken = 0
    profiles = holder.load().get("profiles") or {}
    for page in pages:
        if taken >= size:
            break
        page_id = str(page.get("page_id") or "")
        profile = profiles.get(page_id) or {}
        if str(profile.get("status") or "").upper() != "ACTIVE":
            continue
        if not str(page.get("account_id") or "").strip():
            rows.append({"page_id": page_id, "result": "Page chưa gắn tài khoản."})
            holder.note_attention(page_id, "Page chưa gắn tài khoản.")
            continue
        if _rate_limited_recently(holder, page_id):
            continue
        slots = _slots_needed(holder, posts, profile, page_id, now)
        if not slots:
            _remember_page_health(holder, posts, page_id, [])
            continue
        try:
            made = run_automatic(
                holder,
                pages=[page],
                text_name="openai",
                image_name="openai",
                make_image=True,
                posts=posts,
                slot_times=slots,
                account_index=taken,
            )
        except Exception as exc:  # noqa: BLE001
            rows.append({"page_id": page_id, "result": _safe_error(exc)})
            holder.note_attention(page_id, _safe_error(exc))
            break
        rows.extend(made)
        taken += 1
        _remember_page_health(holder, posts, page_id, made)
    return rows


def _rate_limited_recently(store: FleetStore, page_id: str, minutes: int = 30) -> bool:
    """Vừa bị giới hạn lượt thì chưa viết tiếp, để khỏi gọi API dồn dập."""
    now = datetime.now(timezone.utc)
    for row in reversed(store.load().get("attention") or []):
        if not isinstance(row, dict) or str(row.get("page_id") or "") != page_id:
            continue
        message = str(row.get("message") or "")
        if "429" not in message and "hết credit" not in message:
            return False
        try:
            when = datetime.fromisoformat(str(row.get("at") or "").replace("Z", "+00:00"))
        except ValueError:
            return False
        if when.tzinfo is None:
            when = when.replace(tzinfo=timezone.utc)
        return (now - when.astimezone(timezone.utc)).total_seconds() < minutes * 60
    return False


def _slots_needed(
    store: FleetStore,
    posts: Any,
    profile: dict[str, Any],
    page_id: str,
    now: datetime,
) -> list[datetime]:
    """Giờ còn thiếu của hôm nay và ngày viết trước. Bản thảo kẹt chiếm một chỗ để không viết trùng."""
    days = [now.date() + timedelta(days=_write_ahead_days())]
    if _fill_today():
        days.insert(0, now.date())
    missing: list[datetime] = []
    seen: set[Any] = set()
    for day in days:
        if day in seen:
            continue
        seen.add(day)
        slots = _posting_times(profile, day, now.tzinfo)
        if day == now.date():
            slots = [item for item in slots if item > now]
        used = {(item.hour, item.minute) for item in _jobs_on_day(posts, page_id, day)}
        skipped = _skipped_slots(store, page_id)
        missing.extend(
            item
            for item in slots
            if (item.hour, item.minute) not in used and item.strftime("%Y-%m-%dT%H:%M") not in skipped
        )
    stuck = _stuck_drafts(store, posts, page_id)
    return missing[stuck:]


def _fill_today() -> bool:
    return os.environ.get("FLEET_FILL_TODAY", "1").strip().lower() not in {"0", "false", "off", "no"}


def _remember_page_health(store: FleetStore, posts: Any, page_id: str, rows: list[dict[str, str]]) -> None:
    """Chỉ giữ nhắc khi Page lỗi hoặc còn bản thảo kẹt. Page ổn thì xóa nhắc."""
    issue = ""
    for row in rows:
        result = str(row.get("result") or "")
        if result and not result.startswith("đã vào lịch") and result != "đã đủ bài cho ngày sắp đăng":
            issue = result
            break
    if not issue:
        issue = _stuck_note(store, posts, page_id)
    if issue:
        store.note_attention(page_id, issue)
    else:
        store.clear_attention(page_id)


def _stuck_note(store: FleetStore, posts: Any, page_id: str) -> str:
    if _stuck_drafts(store, posts, page_id) <= 0:
        return ""
    labels = {
        "PENDING_REVIEW": "chờ duyệt",
        "REVISION_REQUIRED": "cần sửa",
        "HUMAN_REVIEW_REQUIRED": "cần người xem",
        "RECHECK_REQUIRED": "cần duyệt lại",
    }
    found: list[str] = []
    handled = {
        str(job.get("fleet_content_id") or "")
        for job in _job_rows(posts)
        if str(job.get("page_id") or "") == page_id and str(job.get("fleet_content_id") or "").strip()
    }
    for content in (store.load().get("contents") or {}).values():
        if str(content.get("page_id") or "") != page_id:
            continue
        if content.get("published") or str(content.get("status") or "") in {"PUBLISHED", "REJECTED"}:
            continue
        if str(content.get("content_id") or "") in handled:
            continue
        found.append(labels.get(str(content.get("status") or ""), "chưa xong"))
    if not found:
        return ""
    return "Có bản thảo chưa vào lịch: " + ", ".join(found[:4]) + "."


def _write_ahead_days() -> int:
    try:
        days = int(os.environ.get("FLEET_WRITE_AHEAD_DAYS", "1") or "1")
    except ValueError:
        days = 1
    return max(1, min(2, days))


def _posting_times(profile: dict[str, Any], day: Any, tz: Any) -> list[datetime]:
    """Rải số bài trong ngày từ 8:00 tới 21:00, hoặc dùng giờ Page đã ghi dạng 08:00, 13:00."""
    daily = max(1, min(10, int(profile.get("daily_content_target") or 1)))
    clocks = _clocks(str(profile.get("posting_schedule") or ""), daily)
    return [datetime(day.year, day.month, day.day, hour, minute, tzinfo=tz) for hour, minute in clocks]


def _clocks(schedule: str, count: int) -> list[tuple[int, int]]:
    found: list[tuple[int, int]] = []
    for hour_text, minute_text in re.findall(r"(\d{1,2}):(\d{2})", schedule or ""):
        hour, minute = int(hour_text), int(minute_text)
        if 0 <= hour <= 23 and 0 <= minute <= 59:
            found.append((hour, minute))
    unique: list[tuple[int, int]] = []
    for pair in found:
        if pair not in unique:
            unique.append(pair)
    if unique:
        return sorted(unique)
    return _spread_clocks(count)


def _spread_clocks(count: int) -> list[tuple[int, int]]:
    count = max(1, min(10, count))
    if count == 1:
        return [(9, 0)]
    start, end = 8 * 60, 21 * 60
    step = (end - start) / (count - 1)
    clocks: list[tuple[int, int]] = []
    for index in range(count):
        minutes = int(round(start + step * index))
        clocks.append((minutes // 60, minutes % 60))
    return clocks


def _jobs_on_day(posts: Any, page_id: str, day: Any) -> list[datetime]:
    found: list[datetime] = []
    for job in _job_rows(posts):
        if str(job.get("page_id") or "") != page_id or not str(job.get("fleet_content_id") or "").strip():
            continue
        if str(job.get("status") or "").lower() not in {"pending", "ready_queue", "running", "success"}:
            continue
        when = _local_datetime(job.get("scheduled_at") or job.get("posted_at"))
        if when is not None and when.date() == day:
            found.append(when)
    return found


def _stuck_drafts(store: FleetStore, posts: Any, page_id: str) -> int:
    handled = {
        str(job.get("fleet_content_id") or "")
        for job in _job_rows(posts)
        if str(job.get("page_id") or "") == page_id and str(job.get("fleet_content_id") or "").strip()
    }
    count = 0
    for content in (store.load().get("contents") or {}).values():
        if str(content.get("page_id") or "") != page_id:
            continue
        if content.get("published") or str(content.get("status") or "") in {"PUBLISHED", "REJECTED"}:
            continue
        if str(content.get("content_id") or "") in handled:
            continue
        count += 1
    return count


def _job_rows(posts: Any) -> list[dict[str, Any]]:
    if not hasattr(posts, "load_all"):
        return []
    return [row for row in posts.load_all() if isinstance(row, dict)]


def _as_utc_iso(when: datetime | None) -> str:
    moment = when or datetime.now(timezone.utc)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def _local_datetime(raw: object) -> datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        when = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return when.astimezone(_local_tz())


def _local_tz() -> Any:
    import os
    from zoneinfo import ZoneInfo

    name = os.environ.get("SCHEDULER_TZ", "Asia/Ho_Chi_Minh").strip() or "Asia/Ho_Chi_Minh"
    return ZoneInfo(name)


def _live_pages() -> list[dict[str, str]]:
    from src.utils.pages_manager import PagesManager

    rows: list[dict[str, str]] = []
    for row in PagesManager().load_all():
        page_id = str(row.get("id") or "").strip()
        if not page_id:
            continue
        rows.append(
            {
                "page_id": page_id,
                "page_name": str(row.get("page_name") or page_id),
                "account_id": str(row.get("account_id") or "").strip(),
            }
        )
    return rows


def _settle_review_task(store: FleetStore, content_id: str, decision: str) -> None:
    for task in store.tasks():
        if str(task.get("content_id") or "") != content_id or str(task.get("state") or "") != "waiting_approval":
            continue
        if decision == "APPROVED":
            transition(store, str(task["task_id"]), "succeeded", note="Đã duyệt")
        elif decision == "REJECTED":
            transition(store, str(task["task_id"]), "cancelled", note="Đã từ chối")
        return


def note_review(store: FleetStore, content_id: str, review: dict[str, Any]) -> None:
    """Ghi kết quả duyệt vào memory của đúng Page. Không tự đổi policy."""
    content = store.content(content_id) or {}
    page_id = str(content.get("page_id") or "")
    if not page_id:
        return
    decision = str(review.get("decision") or "")
    _settle_review_task(store, content_id, decision)
    bucket = "performance_memory" if decision == "APPROVED" else "error_memory"
    store.remember(
        page_id,
        bucket,
        {
            "content_id": content_id,
            "decision": decision,
            "summary": str(review.get("summary") or "")[:300],
        },
    )


def page_report(store: FleetStore, page_id: str) -> dict[str, Any]:
    """Đếm bài và token của một Page. Không gọi API."""
    counts: dict[str, int] = {}
    input_tokens = 0
    output_tokens = 0
    image_calls = 0
    for content in store.load().get("contents", {}).values():
        if str(content.get("page_id") or "") != page_id:
            continue
        status = str(content.get("status") or "DRAFT")
        counts[status] = counts.get(status, 0) + 1
        usage = content.get("usage") or {}
        input_tokens += int(usage.get("input_tokens") or 0)
        output_tokens += int(usage.get("output_tokens") or 0)
        image_calls += int(usage.get("image_calls") or 0)
    profile = (store.load().get("profiles") or {}).get(page_id) or {}
    memory = profile.get("memory") or {}
    return {
        "counts": counts,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "image_calls": image_calls,
        "learned": len(memory.get("performance_memory") or []),
        "errors": len(memory.get("error_memory") or []),
    }


def discard_posts_without_images(store: FleetStore, posts: Any) -> int:
    """Bỏ bài chưa đăng mà không có ảnh, và không viết lại đúng giờ đó."""
    removed = 0
    for content_id, content in list((store.load().get("contents") or {}).items()):
        if not isinstance(content, dict):
            continue
        if content.get("published") or str(content.get("status") or "") in {"PUBLISHED", "REJECTED"}:
            continue
        version = store.current_version(str(content_id)) or {}
        if _asset_file(version):
            continue
        try:
            delete_post(store, str(content_id), posts)
        except Exception:
            continue
        removed += 1
    return removed


def delete_post(store: FleetStore, content_id: str, posts: Any) -> None:
    """Xóa bài chưa đăng, gỡ lịch chờ, và không viết lại đúng giờ đã xóa."""
    content = store.content(content_id)
    if not content:
        raise ValueError("Không thấy bài.")
    if content.get("published") or str(content.get("status") or "") == "PUBLISHED":
        raise ValueError("Bài đã đăng. Không xóa.")
    page_id = str(content.get("page_id") or "")
    removable: list[str] = []
    skipped: list[str] = []
    for job in _job_rows(posts):
        if str(job.get("fleet_content_id") or "") != content_id:
            continue
        status = str(job.get("status") or "").lower()
        if status == "running":
            raise ValueError("Bài đang được đăng. Chưa xóa.")
        when = _local_datetime(job.get("scheduled_at"))
        if when is not None:
            skipped.append(when.strftime("%Y-%m-%dT%H:%M"))
        if status in {"pending", "ready_queue", "failed", ""}:
            removable.append(str(job.get("id") or ""))
    if removable and hasattr(posts, "delete_by_ids"):
        posts.delete_by_ids([item for item in removable if item])
    elif removable:
        for job_id in removable:
            if job_id:
                posts.delete_by_id(job_id)
    store.skip_slots(page_id, skipped)
    store.delete_content(content_id)


def _skipped_slots(store: FleetStore, page_id: str) -> set[str]:
    return {
        str(row.get("slot") or "")
        for row in (store.load().get("skipped_slots") or [])
        if isinstance(row, dict) and str(row.get("page_id") or "") == page_id and str(row.get("slot") or "")
    }


def keep_approval_after_image(store: FleetStore, content_id: str) -> None:
    """Ảnh gắn sau khi đã duyệt thì cập nhật mã duyệt, để lịch đăng không bị chặn."""
    content = store.content(content_id)
    if not content or str(content.get("status") or "") != "APPROVED":
        return
    version = store.current_version(content_id) or {}
    reviews = [dict(row) for row in (content.get("reviews") or []) if isinstance(row, dict)]
    if not reviews or str(reviews[-1].get("decision") or "") != "APPROVED":
        return
    reviews[-1]["approved_content_hash"] = str(version.get("content_hash") or "")
    content["reviews"] = reviews
    store.save_content(content)


def attach_post_image(store: FleetStore, content_id: str, *, image_name: str, posts: Any = None) -> str:
    """Tạo ảnh cho bài chưa đăng. Bài đã duyệt vẫn giữ duyệt và nhận đường ảnh vào lịch."""
    content = store.content(content_id)
    if not content:
        raise ValueError("Không thấy bài.")
    status = str(content.get("status") or "")
    if status in {"PUBLISHED", "REJECTED"}:
        raise RuntimeError("Bài đã đăng hoặc đã từ chối. Không tạo ảnh.")
    asset = _attach_image(store, content, image_name=image_name, allow_approved=status == "APPROVED")
    keep_approval_after_image(store, content_id)
    if posts is not None:
        sync_approved_job(store, content_id, posts)
    return asset


def _attach_image(store: FleetStore, content: dict[str, Any], *, image_name: str, allow_approved: bool = False) -> str:
    chosen = str(image_name or "").strip().lower()
    if chosen not in IMAGE_PROVIDERS:
        raise RuntimeError("Nhà tạo ảnh không có trong danh sách. Không đổi sang nhà khác.")
    version = next(iter(content.get("versions") or []), {})
    prompt = str(version.get("image_prompt") or version.get("caption") or version.get("title") or "").strip()
    if not prompt:
        raise RuntimeError("Bài không có mô tả ảnh.")
    try:
        blobs = AIProviderFactory.image(chosen).generate_images(prompt=prompt, number_of_images=1)
    except Exception as exc:
        detail = _safe_error(exc)
        raise RuntimeError(f"Chưa tạo được ảnh bằng {chosen}: {detail}") from exc
    if not blobs:
        raise RuntimeError(f"Tạo ảnh bằng {chosen} không trả file. Không đổi sang nhà cung cấp khác.")
    page_id = str(content.get("page_id") or "page")
    content_id = str(content.get("content_id") or "post")
    path = save_upload_to_library(blobs[0], f"{page_id}_{content_id}.png")
    store.bind_asset(content_id, path.name, image_provider=chosen, allow_approved=allow_approved)
    return path.name


def _add_usage(store: FleetStore, content_id: str, *, image_calls: int) -> None:
    content = store.content(content_id)
    if not content:
        return
    usage = dict(content.get("usage") or {})
    usage["image_calls"] = int(usage.get("image_calls") or 0) + int(image_calls)
    content["usage"] = usage
    store.save_content(content)


def _safe_error(exc: BaseException) -> str:
    text = str(exc)
    if "sk-" in text or "api_key" in text.casefold() or "Bearer" in text:
        return "Lỗi nhà cung cấp. Chi tiết đã được giấu."
    return text[:240]
