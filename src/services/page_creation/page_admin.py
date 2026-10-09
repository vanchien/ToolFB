"""Thêm quản trị viên cho Page vừa tạo. Không vượt CAPTCHA / checkpoint."""

from __future__ import annotations

import random
import re
from urllib.parse import unquote

from loguru import logger

_ADMIN_ROLE = re.compile(
    r"admin|quản trị|quan tri|full control|toàn quyền|quan ly|quản lý",
    re.I,
)
_ADD_PEOPLE = re.compile(
    r"add (?:people|person|facebook)|thêm người|thêm facebook|invite|mời|give access|cấp quyền",
    re.I,
)
_SEARCH_FIELD = re.compile(
    r"name|email|search|tìm|người|people|facebook",
    re.I,
)


def parse_admin_targets(raw: str) -> list[str]:
    """Tách UID / username / link profile từ ô nhập (xuống dòng hoặc dấu phẩy)."""
    text = (raw or "").replace(";", "\n").replace(",", "\n")
    out: list[str] = []
    seen: set[str] = set()
    for line in text.splitlines():
        token = " ".join(line.split()).strip()
        if not token:
            continue
        key = token.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(token)
    return out


def admin_query_from_target(target: str) -> str:
    """Đưa link profile về UID hoặc username để gõ vào ô tìm."""
    text = (target or "").strip()
    if not text:
        return ""
    if text.isdigit():
        return text
    match = re.search(r"(?:profile\.php\?id=|facebook\.com/)(\d{5,})", text, re.I)
    if match:
        return match.group(1)
    match = re.search(r"facebook\.com/(?:pages/)?([^/?#]+)", text, re.I)
    if match:
        slug = unquote(match.group(1)).strip("/")
        if slug and slug.casefold() not in {"profile.php", "people", "pages"}:
            return slug
    return text


def page_access_urls(page_id: str) -> list[str]:
    """Các URL thường dùng để mở quyền truy cập Page."""
    pid = (page_id or "").strip()
    if not pid:
        return []
    return [
        f"https://www.facebook.com/{pid}/settings/?tab=admin_roles",
        f"https://www.facebook.com/{pid}/settings/?tab=page_roles",
        f"https://www.facebook.com/profile.php?id={pid}&sk=settings",
        f"https://www.facebook.com/{pid}/settings/",
    ]


def is_add_people_label(label: str) -> bool:
    """Nút Thêm người / Add People trên Page settings."""
    text = " ".join((label or "").split()).casefold()
    if not text:
        return False
    return bool(_ADD_PEOPLE.search(text))


def is_admin_role_label(label: str) -> bool:
    """Chọn quyền Admin / Quản trị viên."""
    text = " ".join((label or "").split()).casefold()
    if not text:
        return False
    if any(token in text for token in ("editor", "biên tập", "moderator", "kiểm duyệt", "advertiser", "nhà quảng cáo")):
        return False
    return bool(_ADMIN_ROLE.search(text))


def add_admins_on_page(page, page_id: str, targets: list[str]) -> tuple[int, str]:
    """
    Thêm từng quản trị viên trên Page settings.

    Trả (số đã gửi lời mời, thông báo). Gặp CAPTCHA/checkpoint thì dừng và trả mã trong message.
    """
    wanted = [admin_query_from_target(item) for item in targets if admin_query_from_target(item)]
    if not wanted:
        return 0, ""
    from .facebook_provider import classify_facebook_surface, _human_pause, _step_pause, _shot

    opened = False
    for url in page_access_urls(page_id):
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=60_000)
            _human_pause(page)
        except Exception as exc:  # noqa: BLE001
            logger.debug("Không mở được Page settings {}: {}", url, exc)
            continue
        try:
            body = page.inner_text("body")
        except Exception:  # noqa: BLE001
            body = ""
        blocked = classify_facebook_surface(page.url, body)
        if blocked:
            _shot(page, f"admin_{blocked.lower()}")
            return 0, blocked
        if _page_access_surface_ready(page) or _click_add_people(page):
            opened = True
            break
    if not opened:
        _shot(page, "admin_nav_timeout")
        return 0, "Không mở được mục quyền truy cập Page để thêm quản trị viên."

    added = 0
    notes: list[str] = []
    for query in wanted:
        ok, detail = _invite_one_admin(page, query)
        if detail in {
            "CAPTCHA_DETECTED",
            "CHECKPOINT",
            "SESSION_EXPIRED",
            "RATE_LIMITED",
            "SMS_VERIFICATION_REQUIRED",
        }:
            return added, detail
        if ok:
            added += 1
            notes.append(query)
        else:
            notes.append(f"{query}: {detail or 'không thêm được'}")
        _step_pause(page)
    message = f"Đã gửi quyền quản trị cho {added}/{len(wanted)}: " + "; ".join(notes)
    return added, message


def _page_access_surface_ready(page) -> bool:
    markers = re.compile(r"page access|page roles|quyền truy cập|vai trò trên trang|admin roles", re.I)
    try:
        return page.get_by_text(markers).count() > 0
    except Exception:  # noqa: BLE001
        return False


def _click_add_people(page) -> bool:
    for role in ("button", "link"):
        try:
            loc = page.get_by_role(role)
            count = min(loc.count(), 40)
        except Exception:  # noqa: BLE001
            continue
        for index in range(count):
            item = loc.nth(index)
            try:
                if not item.is_visible():
                    continue
                label = item.inner_text(timeout=600) or item.get_attribute("aria-label") or ""
            except Exception:  # noqa: BLE001
                continue
            if not is_add_people_label(label):
                continue
            try:
                item.click(timeout=5_000)
                return True
            except Exception as exc:  # noqa: BLE001
                logger.debug("Không bấm được Thêm người: {}", exc)
    return False


def _invite_one_admin(page, query: str) -> tuple[bool, str]:
    from .facebook_provider import _human_pause, _step_pause

    _click_add_people(page)
    _step_pause(page)
    box = _search_box(page)
    if box is None:
        return False, "Không thấy ô tìm người"
    try:
        box.click(timeout=4_000)
        try:
            box.press("Control+A")
            box.press("Backspace")
        except Exception:  # noqa: BLE001
            pass
        box.type(query, delay=random.randint(60, 120))
        _step_pause(page)
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)
    if not _pick_search_result(page, query):
        return False, "Không thấy kết quả tìm"
    _step_pause(page)
    _pick_admin_role(page)
    _step_pause(page)
    if not _confirm_invite(page):
        return False, "Không bấm được Cấp quyền"
    _human_pause(page)
    return True, ""


def _search_box(page):
    try:
        named = page.get_by_role("textbox", name=_SEARCH_FIELD)
        if named.count() and named.first.is_visible():
            return named.first
    except Exception:  # noqa: BLE001
        pass
    try:
        boxes = page.get_by_role("textbox")
        for index in range(min(boxes.count(), 8)):
            item = boxes.nth(index)
            if item.is_visible():
                return item
    except Exception:  # noqa: BLE001
        pass
    return None


def _pick_search_result(page, query: str) -> bool:
    needle = query.casefold()
    for role in ("option", "listitem", "row", "button"):
        try:
            loc = page.get_by_role(role)
            count = min(loc.count(), 12)
        except Exception:  # noqa: BLE001
            continue
        for index in range(count):
            item = loc.nth(index)
            try:
                if not item.is_visible():
                    continue
                label = (item.inner_text(timeout=500) or "").casefold()
            except Exception:  # noqa: BLE001
                continue
            if needle in label or (needle.isdigit() and needle in label):
                try:
                    item.click(timeout=4_000)
                    return True
                except Exception:  # noqa: BLE001
                    continue
    try:
        page.keyboard.press("Enter")
        return True
    except Exception:  # noqa: BLE001
        return False


def _pick_admin_role(page) -> bool:
    for role in ("radio", "option", "menuitem", "button"):
        try:
            loc = page.get_by_role(role)
            count = min(loc.count(), 20)
        except Exception:  # noqa: BLE001
            continue
        for index in range(count):
            item = loc.nth(index)
            try:
                if not item.is_visible():
                    continue
                label = item.inner_text(timeout=500) or item.get_attribute("aria-label") or ""
            except Exception:  # noqa: BLE001
                continue
            if not is_admin_role_label(label):
                continue
            try:
                item.click(timeout=4_000)
                return True
            except Exception:  # noqa: BLE001
                continue
    return False


def _confirm_invite(page) -> bool:
    pattern = re.compile(
        r"give access|cấp quyền|gửi lời mời|send invite|add|thêm|xong|done|save|lưu",
        re.I,
    )
    try:
        buttons = page.get_by_role("button", name=pattern)
        count = min(buttons.count(), 8)
    except Exception:  # noqa: BLE001
        return False
    chosen = None
    for index in range(count):
        item = buttons.nth(index)
        try:
            if item.is_visible() and item.is_enabled():
                chosen = item
        except Exception:  # noqa: BLE001
            continue
    if chosen is None:
        return False
    try:
        chosen.click(timeout=5_000)
        return True
    except Exception:  # noqa: BLE001
        return False
