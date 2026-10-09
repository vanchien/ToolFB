"""
Tạo Business Manager cho một account.

Gặp CAPTCHA, checkpoint hoặc tường đăng nhập thì dừng để người dùng xử lý.
Không tự giải, không gọi Graph API, và không bấm hộp «Mời bạn bè» hay «Bắt đầu».
"""

from __future__ import annotations

import random
import re
from urllib.parse import unquote

from loguru import logger

from .facebook_provider import classify_facebook_surface
from .provider import PageCreationOutcome

_BUSINESS_ID = re.compile(r"(?:[?&]business_id=|/business/|portfolio:|bm:)(\d{6,})")
_INVITE = re.compile(r"mời bạn bè|invite(?: your)? friends", re.I)
_CREATE_LABEL = re.compile(
    r"^(?:create(?: a)? business portfolio|create(?: a)? business account|"
    r"tạo danh mục doanh nghiệp|tạo danh mục kinh doanh|tạo tài khoản doanh nghiệp)$",
    re.I,
)
_DISMISS_LABEL = re.compile(r"^(?:hủy|huỷ|cancel|đóng|close|not now|để sau|skip|bỏ qua)$", re.I)
_ADVANCE_LABEL = re.compile(r"^(?:next|tiếp|tiếp tục|continue|create|tạo|submit|xong|done)$", re.I)
_NAME_LABEL = re.compile(
    r"business portfolio name|portfolio name|business name|tên danh mục|tên doanh nghiệp|tên business",
    re.I,
)
_REJECT_FIELD = re.compile(r"mời bạn|invite|search|tìm kiếm|tìm bạn", re.I)
_LABELED_BM_ID = re.compile(
    r"(?:portfolio id|business manager id|business id|id danh mục doanh nghiệp|id doanh nghiệp)\D{0,24}(\d{8,})",
    re.I,
)
_PORTFOLIO_SURFACE = re.compile(
    r"business_scope:(?:portfolio|bm):(\d{6,})(?::([^\"'\s<>]+))?",
    re.I,
)


def extract_business_id(url: str) -> str:
    """Lấy id BM từ URL Business Suite. Không có thì trả chuỗi rỗng."""
    match = _BUSINESS_ID.search(url or "")
    return match.group(1) if match else ""


def business_preview_url(business_id: str = "") -> str:
    """Trang Business Suite để tự tạo BM hoặc xem danh mục đã chọn."""
    bid = (business_id or "").strip()
    if bid:
        return f"https://business.facebook.com/latest/home?business_id={bid}"
    return "https://business.facebook.com/latest/home"


def portfolios_from_markup(markup: str) -> list[dict[str, str]]:
    """Đọc id và tên BM từ HTML/chữ trên Business Suite. Bỏ id của Page."""
    found: dict[str, str] = {}

    def _keep(business_id: str, name: str) -> None:
        bid = (business_id or "").strip()
        if not bid.isdigit() or len(bid) < 6:
            return
        title = " ".join(unquote(name or "").replace("+", " ").split())
        if len(title) > 80 or is_create_portfolio_label(title) or is_invite_friends_dialog(title):
            title = ""
        current = found.get(bid, "")
        if bid not in found or (title and not current):
            found[bid] = title

    for match in _PORTFOLIO_SURFACE.finditer(markup or ""):
        _keep(match.group(1), match.group(2) or "")
    for bid in re.findall(r"(?<![\w])business_id=(\d{6,})", markup or ""):
        _keep(bid, "")
    for match in _LABELED_BM_ID.finditer(markup or ""):
        _keep(match.group(1), "")
    return [{"business_id": bid, "business_name": name} for bid, name in found.items()]


def read_business_portfolios(page) -> tuple[str, list[dict[str, str]]]:
    """
    Mở danh sách danh mục và đọc BM đã có.

    Trả mã chặn (CAPTCHA/checkpoint/hết phiên) hoặc chuỗi rỗng, kèm các BM đọc được.
    """
    from .facebook_provider import _human_pause

    blocked = _surface(page)
    if blocked:
        return blocked, []
    if is_invite_friends_dialog(_dialog_text(page)):
        _dismiss_invite_dialog(page)
        _human_pause(page)
    try:
        page.wait_for_selector("[role='combobox'], [role='main']", timeout=12_000)
    except Exception:  # noqa: BLE001
        pass
    _open_portfolio_switcher(page)
    _scroll_portfolio_list(page)
    rows = portfolios_from_markup(_page_markup(page))
    if rows:
        return "", rows
    try:
        page.goto(
            "https://business.facebook.com/latest/settings/business_info",
            wait_until="domcontentloaded",
            timeout=60_000,
        )
        _human_pause(page)
    except Exception as exc:  # noqa: BLE001
        logger.debug("Không mở được trang thông tin danh mục: {}", exc)
        return "", []
    blocked = _surface(page)
    if blocked:
        return blocked, []
    rows = portfolios_from_markup(_page_markup(page))
    try:
        page.goto(business_preview_url(), wait_until="domcontentloaded", timeout=60_000)
    except Exception:  # noqa: BLE001
        pass
    return "", rows


def _page_markup(page) -> str:
    parts: list[str] = []
    try:
        parts.append(page.content() or "")
    except Exception:  # noqa: BLE001
        pass
    try:
        parts.append(page.inner_text("body") or "")
    except Exception:  # noqa: BLE001
        pass
    try:
        parts.append(page.url or "")
    except Exception:  # noqa: BLE001
        pass
    return "\n".join(parts)


def is_invite_friends_dialog(text: str) -> bool:
    """Hộp mời bạn bè không phải form tạo Business Manager."""
    return bool(_INVITE.search(text or ""))


def _first_line(label: str) -> str:
    return " ".join((label or "").split()).split("\n", 1)[0].strip()


def is_create_portfolio_label(label: str) -> bool:
    """Chỉ nhận nút tạo danh mục. «Bắt đầu» và «Mời bạn bè» không được bấm."""
    return bool(_CREATE_LABEL.match(_first_line(label)))


def is_dismiss_label(label: str) -> bool:
    """Nút đóng hộp mời bạn. Không gồm nút gửi lời mời."""
    return bool(_DISMISS_LABEL.match(_first_line(label)))


def is_wizard_advance_label(label: str) -> bool:
    """Nút tiếp/tạo trên form danh mục. Không khớp «Mời bạn bè»."""
    text = _first_line(label)
    if is_invite_friends_dialog(text):
        return False
    return bool(_ADVANCE_LABEL.match(text))


def is_business_name_field(label: str) -> bool:
    """Ô tên danh mục. Ô tìm bạn trong hộp mời thì bỏ qua."""
    text = label or ""
    if _REJECT_FIELD.search(text):
        return False
    return bool(_NAME_LABEL.search(text))


def new_business_created(before_id: str, after_id: str) -> bool:
    """Chỉ tính là tạo mới khi id sau khác id đang đứng."""
    after = (after_id or "").strip()
    before = (before_id or "").strip()
    return bool(after) and after != before


def create_business_for_account(account_id: str, business_name: str) -> PageCreationOutcome:
    """
    Mở profile của account và tạo Business Manager với tên đã cho.

    Chỉ trả thành công khi đọc được id mới, khác danh mục đang mở.
    """
    from src.automation.browser_factory import BrowserFactory, prepare_playwright_sync_thread, sync_close_persistent_context
    from src.utils.account_proxy_mapper import prepare_account_dict_for_browser_run
    from src.utils.db_manager import AccountsDatabaseManager

    title = " ".join((business_name or "").split())
    if not title:
        return PageCreationOutcome(ok=False, error_code="VALIDATION", error_message="Thiếu tên Business Manager.")
    row = AccountsDatabaseManager().get_by_id(account_id)
    if row is None:
        return PageCreationOutcome(ok=False, error_code="SESSION_EXPIRED", error_message="Không tìm thấy account.")
    factory = None
    context = None
    try:
        prepare_playwright_sync_thread(label=f"bm-create:{account_id}")
        acc = prepare_account_dict_for_browser_run(dict(row), require_proxy_live=True)
        factory = BrowserFactory(headless=False, playwright_shared=True)
        context = factory.launch_persistent_context_from_account_dict(
            acc,
            headless=False,
            disable_notifications=True,
            force_desktop_facebook=True,
        )
        page = context.pages[0] if context.pages else context.new_page()
        return _create_on_page(page, title)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Tạo Business Manager không xong: {}", exc)
        message = str(exc)
        code = "NETWORK_TIMEOUT" if "timeout" in message.lower() or "proxy" in message.lower() else "TEMPORARY_ERROR"
        return PageCreationOutcome(ok=False, error_code=code, error_message=message)
    finally:
        sync_close_persistent_context(context, log_label="bm-create", same_thread=True)
        if factory is not None:
            try:
                factory.close()
            except Exception:  # noqa: BLE001
                pass


def _create_on_page(page, business_name: str) -> PageCreationOutcome:
    from .facebook_provider import _human_pause, _shot

    page.goto("https://business.facebook.com/latest/home", wait_until="domcontentloaded", timeout=60_000)
    _human_pause(page)
    blocked = _surface(page)
    if blocked:
        _shot(page, blocked.lower())
        return PageCreationOutcome(ok=False, error_code=blocked, error_message=blocked)
    if _dialog_text(page) and is_invite_friends_dialog(_dialog_text(page)):
        _dismiss_invite_dialog(page)
        _human_pause(page)
    before = extract_business_id(page.url)
    opened = _open_portfolio_switcher(page)
    if opened:
        _scroll_portfolio_list(page)
    if not _click_create_portfolio(page):
        _shot(page, "bm_create_button")
        return PageCreationOutcome(
            ok=False,
            error_code="NAVIGATION_TIMEOUT",
            error_message=(
                "Không thấy nút Tạo danh mục doanh nghiệp. "
                "Tool không bấm Mời bạn bè hay Bắt đầu trên Meta Business Suite."
            ),
        )
    _human_pause(page)
    blocked = _surface(page)
    if blocked:
        _shot(page, blocked.lower())
        return PageCreationOutcome(ok=False, error_code=blocked, error_message=blocked)
    if is_invite_friends_dialog(_dialog_text(page)):
        _dismiss_invite_dialog(page)
        _shot(page, "bm_invite_dialog")
        return PageCreationOutcome(
            ok=False,
            error_code="NAVIGATION_TIMEOUT",
            error_message="Facebook mở hộp Mời bạn bè, không phải form tạo Business Manager. Hộp đó đã được đóng.",
        )
    if not _fill_business_name(page, business_name):
        _shot(page, "bm_form")
        return PageCreationOutcome(
            ok=False,
            error_code="NAVIGATION_TIMEOUT",
            error_message="Không thấy ô tên danh mục doanh nghiệp.",
        )
    return _submit_until_new_id(page, business_name, before)


def _submit_until_new_id(page, business_name: str, before_id: str) -> PageCreationOutcome:
    from .facebook_provider import _human_pause, _shot

    for _step in range(4):
        blocked = _surface(page)
        if blocked:
            _shot(page, blocked.lower())
            return PageCreationOutcome(ok=False, error_code=blocked, error_message=blocked)
        if is_invite_friends_dialog(_dialog_text(page)):
            _dismiss_invite_dialog(page)
            _shot(page, "bm_invite_dialog")
            return PageCreationOutcome(
                ok=False,
                error_code="NAVIGATION_TIMEOUT",
                error_message="Facebook mở hộp Mời bạn bè trong lúc tạo. Tool không gửi lời mời.",
            )
        after = extract_business_id(page.url)
        if new_business_created(before_id, after):
            return PageCreationOutcome(ok=True, page_id=after, page_url=page.url, error_message=business_name)
        if not _click_matching(page, is_wizard_advance_label):
            break
        _human_pause(page)
        try:
            page.wait_for_timeout(1500)
        except Exception:  # noqa: BLE001
            pass
    after = extract_business_id(page.url)
    if new_business_created(before_id, after):
        return PageCreationOutcome(ok=True, page_id=after, page_url=page.url, error_message=business_name)
    _shot(page, "bm_missing_id")
    return PageCreationOutcome(
        ok=False,
        error_code="NAVIGATION_TIMEOUT",
        error_message=(
            "Chưa tạo được Business Manager mới. "
            "Danh mục đang mở không được tính là vừa tạo. Hãy hoàn tất form nếu còn mở, rồi thử lại."
        ),
    )


def _surface(page) -> str:
    try:
        body = page.inner_text("body")
    except Exception:  # noqa: BLE001
        body = ""
    return classify_facebook_surface(page.url, body)


def _dialog_text(page) -> str:
    try:
        dialog = page.get_by_role("dialog")
        if dialog.count() == 0:
            return ""
        return dialog.first.inner_text(timeout=2_000) or ""
    except Exception:  # noqa: BLE001
        return ""


def _dismiss_invite_dialog(page) -> None:
    """Đóng hộp mời bạn bằng Hủy/Đóng. Không bấm Mời bạn bè."""
    if not is_invite_friends_dialog(_dialog_text(page)):
        return
    _click_matching(page, is_dismiss_label, roles=("button",))


def _open_portfolio_switcher(page) -> bool:
    from src.automation.meta_business_scanner import _open_business_scope_selector

    try:
        return bool(_open_business_scope_selector(page, lambda message: logger.info("[BM] {}", message)))
    except Exception as exc:  # noqa: BLE001
        logger.debug("Không mở được danh sách danh mục: {}", exc)
        return False


def _scroll_portfolio_list(page) -> None:
    from src.automation.meta_business_scanner import _scroll_left_scope_switcher_list_full

    try:
        _scroll_left_scope_switcher_list_full(page)
    except Exception as exc:  # noqa: BLE001
        logger.debug("Không cuộn được danh sách danh mục: {}", exc)


def _click_create_portfolio(page) -> bool:
    if _click_matching(page, is_create_portfolio_label, roles=("button", "link", "menuitem", "row")):
        return True
    pattern = re.compile(
        r"Create a business portfolio|Create a business account|"
        r"Tạo danh mục doanh nghiệp|Tạo danh mục kinh doanh|Tạo tài khoản doanh nghiệp",
        re.I,
    )
    try:
        matches = page.get_by_text(pattern)
        count = min(matches.count(), 8)
    except Exception:  # noqa: BLE001
        return False
    for index in range(count):
        item = matches.nth(index)
        try:
            text = item.inner_text(timeout=1_000)
            if not is_create_portfolio_label(text) or len(text) > 80:
                continue
            item.click(timeout=8_000)
            return True
        except Exception:  # noqa: BLE001
            continue
    return False


def _click_matching(page, predicate, *, roles: tuple[str, ...] = ("button", "link")) -> bool:
    for role in roles:
        try:
            loc = page.get_by_role(role)
            count = min(loc.count(), 50)
        except Exception:  # noqa: BLE001
            continue
        for index in range(count):
            item = loc.nth(index)
            try:
                if not item.is_visible():
                    continue
                text = item.inner_text(timeout=800)
            except Exception:  # noqa: BLE001
                continue
            if not predicate(text):
                continue
            try:
                item.click(timeout=8_000)
                return True
            except Exception as exc:  # noqa: BLE001
                logger.debug("Không bấm được «{}»: {}", _first_line(text), exc)
    return False


def _fill_business_name(page, business_name: str) -> bool:
    if is_invite_friends_dialog(_dialog_text(page)):
        return False
    pattern = re.compile(
        r"business portfolio name|portfolio name|business name|tên danh mục|tên doanh nghiệp|tên business",
        re.I,
    )
    try:
        box = page.get_by_role("textbox", name=pattern)
        if box.count() == 0:
            return False
        target = box.first
        target.click(timeout=8_000)
        page.wait_for_timeout(random.randint(800, 1600))
        try:
            target.press("Control+A")
        except Exception:  # noqa: BLE001
            pass
        target.type(business_name, delay=random.randint(40, 110))
        return True
    except Exception as exc:  # noqa: BLE001
        logger.debug("Không điền được tên Business Manager: {}", exc)
        return False
