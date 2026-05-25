"""
Hợp nhất cấu hình AI/lịch từ job queue (``schedule_posts.json``) với bản ghi Page.

Dùng bởi ``scheduler.run_scheduled_post_for_account`` khi có ``schedule_post_job_id``.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Literal

from src.utils.page_schedule import parse_cron_hh_mm, scheduler_tz


def _parse_iso_to_aware_utc(raw: str) -> datetime:
    s = str(raw or "").strip().replace("Z", "+00:00")
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def compute_next_daily_scheduled_utc_iso(schedule_slot: str, *, from_utc: datetime | None = None) -> str:
    """Lần chạy tiếp theo (UTC ISO) cho lịch ``HH:MM`` mỗi ngày theo ``SCHEDULER_TZ``."""
    tz = scheduler_tz()
    now_u = from_utc or datetime.now(timezone.utc)
    now_l = now_u.astimezone(tz)
    h, m = parse_cron_hh_mm(schedule_slot)
    cand = now_l.replace(hour=h, minute=m, second=0, microsecond=0)
    if cand <= now_l:
        cand = cand + timedelta(days=1)
    return cand.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def once_local_wall_to_utc_iso(once_local_yyyy_mm_dd_hh_mm: str) -> str:
    """``YYYY-MM-DD HH:MM`` (wall ``SCHEDULER_TZ``) → UTC ISO."""
    from src.utils.page_schedule import parse_once_local

    tz = scheduler_tz()
    dt = parse_once_local(once_local_yyyy_mm_dd_hh_mm, tz)
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def internal_post_title_from_body(body: str, *, fallback: str = "") -> str:
    """Dòng đầu tiên khác rỗng của văn bản (bỏ hẳn các dòng sau). Dùng cho tiêu đề nội bộ và ``content`` khi chỉ đăng một dòng caption."""
    for line in str(body or "").replace("\r\n", "\n").replace("\r", "\n").split("\n"):
        s = line.strip()
        if s:
            return s
    return str(fallback or "").strip()


_HASHTAG_IN_TEXT_RE = re.compile(r"#[\w\u00C0-\u024F\u1E00-\u1EFF]+", re.UNICODE)


def _normalize_cmp_text(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "").strip()).lower()


def _strip_title_prefix_from_content(title: str, content: str) -> str:
    """Bỏ tiêu đề lặp ở đầu nội dung (không phân biệt hoa thường)."""
    title_s = str(title or "").strip()
    content_s = str(content or "").strip()
    if not title_s or not content_s:
        return content_s
    pat = re.compile(
        r"^\s*" + re.escape(title_s) + r"\s*[\-:|—\n]*",
        re.IGNORECASE,
    )
    out = pat.sub("", content_s, count=1).strip()
    if out:
        return out
    if content_s.lower().startswith(title_s.lower()):
        return content_s[len(title_s) :].lstrip(" \n\r\t-:|—")
    return content_s


def dedupe_post_title_content(title: str, content: str) -> tuple[str, str]:
    """
    Loại trùng giữa tiêu đề và nội dung — chỉ giữ một phần khi trùng hoàn toàn hoặc lồng nhau.

    - Giống hệt → giữ tiêu đề, bỏ nội dung
    - Nội dung bắt đầu bằng tiêu đề → bỏ prefix trùng; nếu còn rỗng → chỉ tiêu đề
    - Tiêu đề chứa toàn bộ nội dung → chỉ tiêu đề
    - Nội dung chứa toàn bộ tiêu đề (không chỉ prefix) → chỉ nội dung
    """
    title_s = str(title or "").strip()
    content_s = str(content or "").strip()
    if not title_s:
        return "", content_s
    if not content_s:
        return title_s, ""
    nt = _normalize_cmp_text(title_s)
    nc = _normalize_cmp_text(content_s)
    if nt == nc:
        return title_s, ""
    if nc.startswith(nt):
        rest = _strip_title_prefix_from_content(title_s, content_s)
        if not rest or _normalize_cmp_text(rest) == nt:
            return title_s, ""
        return title_s, rest
    if nt.startswith(nc):
        return title_s, ""
    if nt in nc:
        return "", content_s
    if nc in nt:
        return title_s, ""
    return title_s, content_s


def _normalize_hashtag_token(raw: str) -> str:
    s = str(raw or "").strip()
    if not s:
        return ""
    if not s.startswith("#"):
        s = "#" + s.lstrip("#")
    return s.replace(" ", "")


def _hashtag_keyword(tag: str) -> str:
    """Từ khóa so sánh trùng (bỏ ``#``, hoa thường, khoảng trắng)."""
    return str(tag or "").strip().lstrip("#").lower().replace(" ", "").replace("_", "")


def _iter_hashtag_raw_tokens(hashtags: list[str] | str | None) -> list[str]:
    """Tách token hashtag từ list/string (hỗ trợ dấu phẩy, chấm phẩy, xuống dòng)."""
    if isinstance(hashtags, str):
        raw_items: list[str] = [hashtags]
    elif isinstance(hashtags, list):
        raw_items = [str(x or "") for x in hashtags]
    else:
        return []
    tokens: list[str] = []
    for chunk in raw_items:
        for part in re.split(r"[,;\n]+", str(chunk)):
            for token in part.split():
                t = str(token or "").strip()
                if t:
                    tokens.append(t)
    return tokens


def dedupe_hashtag_keywords(tags: list[str]) -> list[str]:
    """
    Hashtag trùng **từ khóa** → giữ một (tag đầu tiên); không trùng → giữ nguyên thứ tự.
    """
    out: list[str] = []
    seen_kw: set[str] = set()
    for raw in tags:
        tag = _normalize_hashtag_token(raw)
        if not tag:
            continue
        kw = _hashtag_keyword(tag)
        if not kw or kw in seen_kw:
            continue
        seen_kw.add(kw)
        out.append(tag)
    return out


def _normalize_hashtag_list(hashtags: list[str] | str | None) -> list[str]:
    tokens = _iter_hashtag_raw_tokens(hashtags)
    return dedupe_hashtag_keywords([_normalize_hashtag_token(t) for t in tokens if t])


def _extract_hashtags_from_text(text: str) -> list[str]:
    found = [_normalize_hashtag_token(m.group(0)) for m in _HASHTAG_IN_TEXT_RE.finditer(str(text or ""))]
    return dedupe_hashtag_keywords([t for t in found if t])


def _remove_hashtags_from_text(text: str, tags: list[str]) -> str:
    s = str(text or "").strip()
    if not s or not tags:
        return s
    out = s
    for tag in tags:
        if not tag:
            continue
        pat = re.compile(re.escape(tag) + r"(?=\s|$|[,.;!?])", re.IGNORECASE)
        out = pat.sub("", out)
    out = re.sub(r"[ \t]{2,}", " ", out)
    out = re.sub(r"\n{3,}", "\n\n", out)
    return out.strip()


def dedupe_post_title_content_hashtags(
    title: str,
    content: str,
    hashtags: list[str] | str | None,
) -> tuple[str, str, list[str]]:
    """
    Dedupe title/content; gom hashtag từ field + text; gỡ hashtag khỏi title/content nếu đã có trong list.
    """
    title_s, content_s = dedupe_post_title_content(title, content)
    tags = _normalize_hashtag_list(hashtags)
    seen_kw = {_hashtag_keyword(t) for t in tags}
    for extra in _extract_hashtags_from_text(title_s) + _extract_hashtags_from_text(content_s):
        kw = _hashtag_keyword(extra)
        if not kw or kw in seen_kw:
            continue
        seen_kw.add(kw)
        tags.append(extra)
    tags = dedupe_hashtag_keywords(tags)
    title_s = _remove_hashtags_from_text(title_s, tags)
    content_s = _remove_hashtags_from_text(content_s, tags)
    title_s, content_s = dedupe_post_title_content(title_s, content_s)
    return title_s, content_s, tags


def strip_image_note_from_text(text: str) -> str:
    """Loại dòng chú thích ảnh ``(Ảnh: ...)`` / ``(Image: ...)`` khỏi caption."""
    raw = str(text or "").strip()
    if not raw:
        return raw
    out_lines: list[str] = []
    for ln in raw.splitlines():
        s = ln.strip().lower()
        if s.startswith("(ảnh:") or s.startswith("(anh:") or s.startswith("(image:"):
            continue
        out_lines.append(ln)
    cleaned = "\n".join(out_lines).strip()
    return cleaned or raw


def compose_caption_from_deduped(
    title: str,
    content: str,
    hashtags: list[str],
    *,
    append_hashtags: bool = True,
) -> str:
    """Ghép caption text/image sau dedupe — không lặp title/content/hashtag."""
    title_s, content_s = dedupe_post_title_content(title, content)
    if title_s and content_s:
        base = f"{title_s}\n\n{content_s}".strip()
    elif title_s:
        base = title_s
    else:
        base = content_s
    if not append_hashtags or not hashtags:
        return base
    joined = " ".join(hashtags)
    if not joined:
        return base
    if joined.lower() in base.lower():
        return base
    return (base + "\n\n" + joined).strip() if base else joined


def reel_tags_from_hashtags(hashtags: list[str], *, limit: int = 12) -> list[str]:
    """Hashtag có ``#`` → keyword Reel Tags (dedupe từ khóa)."""
    out: list[str] = []
    seen: set[str] = set()
    for tag in dedupe_hashtag_keywords(hashtags):
        kw = _hashtag_keyword(tag)
        if not kw or kw in seen:
            continue
        seen.add(kw)
        out.append(kw[:80])
        if len(out) >= limit:
            break
    return out


def prepare_queue_job_post_fields(
    queue_job: dict[str, Any] | None,
    *,
    fallback_body: str = "",
) -> dict[str, Any]:
    """
    Chuẩn hóa một lần title / content / hashtag cho job lịch đăng.

    Trả về dict: ``title``, ``content``, ``hashtags``, ``reel_tags``, ``caption_text``,
    ``reel_description``, ``post_type``.
    """
    if not queue_job:
        fb = strip_image_note_from_text(fallback_body)
        return {
            "title": "",
            "content": fb,
            "hashtags": [],
            "reel_tags": [],
            "caption_text": fb,
            "reel_description": fb,
            "post_type": "text",
        }
    pt = str(queue_job.get("post_type", "text")).strip().lower()
    title_raw = str(queue_job.get("title") or "").strip()
    content_raw = strip_image_note_from_text(str(queue_job.get("content") or ""))
    raw_tags = queue_job.get("tags")
    if not isinstance(raw_tags, list):
        raw_tags = queue_job.get("hashtags")

    is_reel_like = pt in {"video", "text_video", "reel"}
    if is_reel_like:
        body_src = content_raw
    else:
        body_src = content_raw if content_raw else strip_image_note_from_text(fallback_body)

    title, content, hashtags = dedupe_post_title_content_hashtags(
        title_raw,
        body_src,
        raw_tags if isinstance(raw_tags, (list, str)) else None,
    )
    reel_tags = reel_tags_from_hashtags(hashtags)

    if title and content:
        reel_description = f"{title}\n\n{content}".strip()
    elif title:
        reel_description = title
    else:
        reel_description = content

    if is_reel_like:
        caption_text = content
    else:
        caption_text = compose_caption_from_deduped(title, content, hashtags)

    return {
        "title": title,
        "content": content,
        "hashtags": hashtags,
        "reel_tags": reel_tags,
        "caption_text": caption_text,
        "reel_description": reel_description,
        "post_type": pt,
    }


def merge_queue_job_content_into_page_row(
    page_row: dict[str, Any] | None,
    queue_job: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Job có ``ai_*`` / ``ai_config`` / ``job_post_image_path`` → ghi đè trường tương ứng trên Page (bản sao)."""
    if not page_row:
        return page_row
    if not queue_job:
        return page_row
    out = dict(page_row)
    if str(queue_job.get("ai_topic", "")).strip():
        out["topic"] = str(queue_job["ai_topic"]).strip()
    if str(queue_job.get("ai_content_style", "")).strip():
        out["content_style"] = str(queue_job["ai_content_style"]).strip()
    if str(queue_job.get("job_post_image_path", "")).strip():
        out["post_image_path"] = str(queue_job["job_post_image_path"]).strip()
    if str(queue_job.get("page_url", "")).strip():
        out["page_url"] = str(queue_job["page_url"]).strip()
    cfg = queue_job.get("ai_config")
    if isinstance(cfg, dict):
        parts: list[str] = []
        bv = str(cfg.get("brand_voice", "")).strip()
        if bv:
            parts.append(bv)
        ta = str(cfg.get("target_audience", "")).strip()
        if ta:
            parts.append(f"Đối tượng đọc: {ta}")
        pl = cfg.get("content_pillars")
        if isinstance(pl, list) and pl:
            parts.append("Trụ cột nội dung: " + ", ".join(str(x) for x in pl[:8]))
        av = cfg.get("avoid_keywords")
        if isinstance(av, list) and av:
            parts.append("Không dùng từ: " + ", ".join(str(x) for x in av[:12]))
        if parts:
            base_style = str(out.get("content_style", "")).strip()
            out["content_style"] = " | ".join(parts + ([base_style] if base_style else []))
    return out


def deserialize_job_schedule_for_ui(job: dict[str, Any]) -> tuple[Literal["once", "daily"], date, int, int]:
    """Đổ widget lịch từ job đã lưu."""
    rec = str(job.get("schedule_recurrence", "")).strip().lower()
    slot = str(job.get("schedule_slot", "")).strip()
    tz = scheduler_tz()
    if rec == "daily" and slot:
        h, m = parse_cron_hh_mm(slot)
        return ("daily", datetime.now(tz).date(), h, m)
    raw = str(job.get("scheduled_at", "")).strip()
    if raw:
        dtu = _parse_iso_to_aware_utc(raw)
        loc = dtu.astimezone(tz)
        return ("once", loc.date(), loc.hour, loc.minute)
    return ("once", datetime.now(tz).date(), 9, 0)


def build_schedule_slot_hhmm(hour: int, minute: int) -> str:
    from src.utils.page_schedule import normalize_hh_mm

    return normalize_hh_mm(hour, minute)
