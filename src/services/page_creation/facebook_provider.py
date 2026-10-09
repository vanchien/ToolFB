"""
Tạo Facebook Page trên Business Manager (tên + avatar).

Gặp CAPTCHA, checkpoint, đăng nhập lại hoặc giới hạn của nền tảng thì dừng và trả mã
để người dùng xử lý. Không gọi bộ giải CAPTCHA và không tự vượt bước xác minh.
"""

from __future__ import annotations

import random
import re
import threading
import time
from pathlib import Path
from urllib.parse import unquote

from loguru import logger

from src.automation.facebook_actions import _facebook_url_is_security_interstitial
from src.utils.paths import project_root

from .provider import PageCreationOutcome, PageCreationRequest

_SMS_MARKERS = (
    "finish sms verification",
    "sms verification on mobile",
    "sms verification",
    "we noticed suspicious activity",
    "before creating a new page",
    "xác minh qua sms",
    "xác minh sms",
    "hoàn tất xác minh sms",
    "ứng dụng di động trước khi tạo",
)
_CAPTCHA_MARKERS = (
    "recaptcha",
    "captcha",
    "i'm not a robot",
    "tôi không phải là người máy",
    "xác nhận bạn là người",
)
_CHECKPOINT_MARKERS = (
    "checkpoint",
    "identity verification",
    "xác minh danh tính",
    "security check",
    "kiểm tra bảo mật",
    "confirm your identity",
)
_RATE_MARKERS = (
    "rate limit",
    "too many attempts",
    "try again later",
    "thử lại sau",
    "quá nhiều lần",
    "temporarily blocked",
)

SMS_USER_MESSAGE = (
    "Facebook yêu cầu hoàn tất SMS verification trên mobile app."
)

# Tổng bước hiển thị cho người dùng (tạo form + hậu tạo).
_STEP_TOTAL = 10


def _notify_progress(
    request: PageCreationRequest,
    message: str,
    *,
    step: int | None = None,
    state: str | None = None,
) -> None:
    """Đẩy trạng thái + dòng tiến độ từng bước lên UI (job.progress_message)."""
    if state:
        try:
            request.on_state(state)
        except Exception:  # noqa: BLE001
            pass
    text = str(message or "").strip()
    if not text:
        return
    if step is not None and step > 0:
        text = f"Bước {int(step)}/{_STEP_TOTAL}: {text}"
    try:
        request.on_progress(text)
    except Exception:  # noqa: BLE001
        pass
    logger.info("[PageCreator] {}", text)


def user_message_for_code(code: str) -> str:
    """Thông báo người dùng cho mã dừng bảo mật."""
    if code == "SMS_VERIFICATION_REQUIRED":
        return SMS_USER_MESSAGE
    if code == "CAPTCHA_DETECTED":
        return "Facebook đang hiện CAPTCHA. Hãy xử lý trên trình duyệt rồi bấm Kiểm tra lại."
    if code == "CHECKPOINT":
        return "Facebook đang yêu cầu checkpoint / xác minh danh tính. Hãy xử lý trên trình duyệt rồi bấm Kiểm tra lại."
    if code == "SESSION_EXPIRED":
        return "Phiên đăng nhập đã hết. Hãy đăng nhập lại rồi bấm Kiểm tra lại."
    if code == "RATE_LIMITED":
        return "Facebook đang giới hạn thao tác. Hãy đợi rồi bấm Kiểm tra lại."
    return code or "Cần người dùng xử lý trên trình duyệt."


def classify_facebook_surface(url: str, body_text: str) -> str:
    """
    Đọc URL + chữ trên trang.

    Trả mã lỗi cần người dùng, hoặc chuỗi rỗng nếu không thấy chặn.
    Không vượt SMS / CAPTCHA / checkpoint.
    """
    blob = f"{url or ''}\n{body_text or ''}".lower()
    if _looks_like_sms_verification(blob):
        return "SMS_VERIFICATION_REQUIRED"
    if any(marker in blob for marker in _CAPTCHA_MARKERS):
        return "CAPTCHA_DETECTED"
    lowered_url = (url or "").lower()
    if any(marker in blob for marker in _CHECKPOINT_MARKERS) or (
        _facebook_url_is_security_interstitial(url) and "login" not in lowered_url
    ):
        return "CHECKPOINT"
    if any(marker in blob for marker in _RATE_MARKERS):
        return "RATE_LIMITED"
    if "facebook.com" in lowered_url and ("/login" in lowered_url or "login.php" in lowered_url):
        return "SESSION_EXPIRED"
    return ""


def _looks_like_sms_verification(blob: str) -> bool:
    """Màn «suspicious activity» + yêu cầu SMS trên app trước khi tạo Page."""
    if "finish sms verification" in blob or "sms verification on mobile" in blob:
        return True
    if "we noticed suspicious activity" in blob and (
        "sms" in blob or "mobile app" in blob or "creating a new page" in blob
    ):
        return True
    if "xác minh" in blob and "sms" in blob:
        return True
    return any(marker in blob for marker in _SMS_MARKERS if marker not in {"before creating a new page"})


_CONFIRM_EXACT = {"create page", "create", "tạo trang", "tạo", "next", "tiếp", "tiếp tục", "save", "lưu", "done", "xong"}
_WIZARD_ACTION_NAME = re.compile(
    r"^\s*(?:next|tiếp(?:\s+theo)?|tiếp tục|tạo trang|create(?:\s+page)?|xong|done|save|lưu)\s*$",
    re.I,
)
_BLOCKED_CLICK = (
    "mời bạn",
    "invite friend",
    "invite your friend",
    "bắt đầu",
    "get started",
    "yêu cầu quyền",
    "request access",
    "quyền truy cập",
    "có sẵn",
    "existing page",
    "add an existing",
    "add existing",
)
_PAGE_NAME_FIELD = re.compile(r"page name|tên trang|tên page", re.I)
_BIO_FIELD = re.compile(
    r"bio|description|tiểu sử|tieu su|mô tả|mo ta|giới thiệu|gioi thieu|"
    r"tell people|about your page|what your page is about",
    re.I,
)
_REJECT_FIELD = re.compile(r"search|tìm kiếm|tìm bạn|mời bạn|invite", re.I)
_PAGE_SURFACE = re.compile(r"business_scope:page:(\d{8,}):([^\"'\s<>]+)", re.I)
_ASSET_ID = re.compile(r"(?<![\w])asset_id=(\d{8,})")
_ADD_MENU_LABEL = re.compile(r"^(?:add|thêm)$", re.I)


def _first_line(label: str) -> str:
    return " ".join((label or "").split()).split("\n", 1)[0].strip()


def is_add_menu_label(label: str) -> bool:
    """Nút «+ Add» trên danh sách Pages. Không nhận Assign people hay Add existing."""
    text = _first_line(label).lstrip("+＋").strip()
    return bool(_ADD_MENU_LABEL.fullmatch(text))


_PAGE_SWITCH_PROMPT = re.compile(
    r"switch into .+page to (?:start managing|take more actions)|"
    r"chuyển (?:sang|vào) .+(?:để quản lý|để bắt đầu|để thực hiện)",
    re.I,
)


def page_switch_prompt_text(text: str) -> bool:
    """Banner yêu cầu Switch vào Page vừa tạo trước khi sửa ảnh và thông tin."""
    return bool(_PAGE_SWITCH_PROMPT.search(text or ""))


def is_switch_now_label(label: str) -> bool:
    """Nút xanh Switch Now trên trang Page. Không nhận Switch tài khoản khác."""
    return _first_line(label).casefold().strip(" .") in {"switch now", "chuyển ngay", "chuyển đổi ngay"}


def is_page_switch_label(label: str) -> bool:
    """Nút Switch trong banner hoặc hộp xác nhận. Không nhận Switch Now."""
    return _first_line(label).casefold().strip(" .") in {"switch", "chuyển", "chuyển đổi"}


def _blocked_click(label: str) -> bool:
    text = _first_line(label).casefold()
    return any(marker in text for marker in _BLOCKED_CLICK)


def is_create_page_label(label: str) -> bool:
    """Chỉ «Tạo Trang Facebook mới». Không nhận Thêm Trang có sẵn hay xin quyền."""
    text = _first_line(label).casefold()
    if not text or text in {"add", "thêm"} or _blocked_click(text):
        return False
    vietnamese = "tạo" in text and "trang" in text and "mới" in text
    english = "create" in text and "page" in text and "new" in text
    return vietnamese or english


def is_confirm_page_label(label: str) -> bool:
    """Nút Next / Tiếp / Tạo Trang trên form. Không bấm Hủy, Quay lại hay «Tạo Trang Facebook mới»."""
    return confirm_button_rank(label) > 0


def confirm_button_rank(label: str) -> int:
    """Ưu tiên nút Tạo Trang ở bước Xác nhận hơn nút bước «Xác nhận» trên thanh trái."""
    text = _first_line(label).casefold().strip(" .")
    if not text or text in {"hủy", "huỷ", "cancel", "đóng", "close", "back", "quay lại"}:
        return 0
    if _blocked_click(text) or "mới" in text or "new page" in text or "facebook mới" in text:
        return 0
    if text in {"tạo trang", "create page"}:
        return 3
    if text in _CONFIRM_EXACT or text in {"tiếp theo", "xong"} or text.split()[0] in {"next", "tiếp"}:
        return 2
    if text in {"xác nhận", "confirm"}:
        return 1
    return 0


def is_terms_checkbox_label(label: str) -> bool:
    """Ô «tôi đồng ý» trên bước Xác nhận. Không nhận tiêu đề Quyền truy cập."""
    text = " ".join((label or "").split()).casefold()
    if not text:
        return False
    return any(
        token in text
        for token in ("đồng ý", "dong y", "i agree", "i accept", "điều khoản", "commercial terms")
    )


def name_was_entered(value: str, expected: str) -> bool:
    """Ô tên đã giữ đúng chữ vừa gõ, không chỉ mới nhận phím."""
    typed = " ".join((value or "").split()).casefold()
    wanted = " ".join((expected or "").split()).casefold()
    return bool(wanted) and wanted in typed


def _category_text_variants(label: str) -> list[str]:
    """Tách nhãn. «Product/service Product/service» (chữ + aria-label) vẫn ra đúng một tên."""
    text = " ".join(_first_line(label).casefold().replace("／", "/").split())
    if not text:
        return []
    variants = [text]
    parts = text.split()
    if len(parts) >= 2 and len(parts) % 2 == 0:
        half = len(parts) // 2
        if parts[:half] == parts[half:]:
            variants.append(" ".join(parts[:half]))
    return variants


def category_option_matches(label: str, wanted: str | None = None) -> bool:
    """Khớp đúng tên hạng mục. Không nhận chuỗi con và không chọn Legal / Local service / Shopping."""
    variants = _category_text_variants(label)
    if not variants or any(_blocked_click(text) for text in variants):
        return False
    blocked = (
        "legal",
        "local service",
        "shopping",
        "restaurant",
        "thêm trang",
        "add an existing",
        "invite",
        "mời",
    )
    if any(any(token in text for token in blocked) for text in variants):
        return False
    from .page_details import categories_are_same, category_pool_labels

    if wanted:
        return any(categories_are_same(text, wanted) for text in variants)
    pool = [" ".join(item.casefold().replace("／", "/").split()) for item in category_pool_labels()]
    if any(text in pool for text in variants):
        return True
    text = variants[0]
    vietnamese = ("sản phẩm" in text or "san pham" in text) and ("dịch vụ" in text or "dich vu" in text)
    english = bool(re.search(r"\bproducts?\b", text) and re.search(r"\bservices?\b", text))
    return vietnamese or english


def is_bio_field_label(label: str) -> bool:
    """Ô Bio trên form tạo Page. Không nhận ô tên hay ô hạng mục."""
    text = label or ""
    if re.search(r"page name|tên trang|tên page|category|hạng mục|danh mục", text, re.I):
        return False
    return bool(_BIO_FIELD.search(text))


def parse_suggested_page_name(blob: str) -> str:
    """Lấy tên Facebook gợi ý từ thông báo lỗi (EN/VI)."""
    text = " ".join((blob or "").split())
    if not text:
        return ""
    patterns = (
        r"(?:suggested|đề xuất|goi y|gợi ý)\s+['\u2018\u2019\"]([^'\u2018\u2019\"]+)['\u2018\u2019\"]",
        r"(?:suggested|đề xuất)\s+[«]?([^»'\"]+)[»]?",
    )
    for pattern in patterns:
        match = re.search(pattern, text, re.I)
        if match:
            return " ".join(match.group(1).split()).strip(" .")
    return ""


def name_feedback_is_invalid(blob: str) -> bool:
    """True khi form báo tên Page không hợp lệ."""
    text = blob or ""
    return bool(
        re.search(
            r"is invalid|không hợp lệ|not allowed|không được phép|we have suggested|đã đề xuất",
            text,
            re.I,
        )
    )


def is_page_name_field(label: str) -> bool:
    """Ô tên Page. Bỏ ô tìm kiếm và ô mời bạn."""
    text = label or ""
    if _REJECT_FIELD.search(text):
        return False
    return bool(_PAGE_NAME_FIELD.search(text))


_CREATE_CONFIRM_MARKERS = re.compile(
    r"are you sure you want to continue|"
    r"the page will be created|"
    r"i agree to meta commercial|"
    r"pages, groups and events policies|"
    r"tôi đồng ý với điều khoản|"
    r"điều khoản thương mại của meta",
    re.I,
)


def markup_is_create_confirm(markup: str) -> bool:
    """Hộp «Are you sure…» / đồng ý điều khoản — Page chưa được tạo."""
    return bool(_CREATE_CONFIRM_MARKERS.search(markup or ""))


def created_page_id_from_url(url: str) -> str:
    """Id Page trên URL sau khi tạo. Không lấy ``business_id`` của Business Suite."""
    text = url or ""
    business_ids = set(re.findall(r"business_id=(\d{6,})", text, re.I))
    host_business = "business.facebook.com" in text.lower()
    patterns = [
        r"profile\.php\?id=(\d{8,})",
        r"facebook\.com/pages/[^/?#]+/(\d{8,})",
        r"[?&]page_id=(\d{8,})",
        r"[?&]selected_asset_id=(\d{8,})",
        r"[?&]asset_id=(\d{8,})",
    ]
    if not host_business:
        patterns.extend(
            [
                r"facebook\.com/(\d{8,})(?:[/?#]|$)",
                r"[?&]id=(\d{8,})",
            ]
        )
    for pattern in patterns:
        match = re.search(pattern, text, re.I)
        if match and match.group(1) not in business_ids:
            return match.group(1)
    return ""


def page_match_from_markup(markup: str, page_name: str) -> tuple[str, str]:
    """Lấy id Page đúng tên. Không lấy business id hay Page khác trong danh sách."""
    target = " ".join((page_name or "").split()).casefold()
    if not target:
        return "", ""
    for match in _PAGE_SURFACE.finditer(markup or ""):
        name = " ".join(unquote(match.group(2)).replace("+", " ").split())
        if name.casefold() == target:
            page_id = match.group(1)
            return page_id, f"https://www.facebook.com/{page_id}"
    near = page_id_near_name(markup, page_name)
    if near:
        return near, f"https://www.facebook.com/{near}"
    # Hộp xác nhận còn trong HTML thì không lấy asset_id lẻ — Page chưa được tạo.
    if markup_is_create_confirm(markup):
        return "", ""
    # Tên chỉ nằm trong ô tìm thì chưa phải Page. Không lấy UID của trang đang mở.
    if not _name_hits_outside_fields(markup, target):
        return "", ""
    ids = list(dict.fromkeys(_ASSET_ID.findall(markup or "")))
    if len(ids) == 1 and target in (markup or "").casefold():
        page_id = ids[0]
        return page_id, f"https://www.facebook.com/{page_id}"
    return "", ""


def page_id_near_name(markup: str, page_name: str) -> str:
    """UID nằm cạnh đúng tên Page. Bỏ qua đoạn chữ của hộp «Are you sure»."""
    target = " ".join((page_name or "").split()).casefold()
    if not target:
        return ""
    business_ids = set(re.findall(r"business_id=(\d{6,})", markup or "", re.I))
    haystack = (markup or "").casefold()
    pattern = re.compile(
        r"(?:selected_asset_id|asset_id|page_id)=(\d{8,})|business_scope:page:(\d{8,})",
        re.I,
    )
    start = 0
    while True:
        index = haystack.find(target, start)
        if index < 0:
            return ""
        window = (markup or "")[max(0, index - 600) : index + len(target) + 600]
        if not _hit_inside_field(markup or "", index) and not markup_is_create_confirm(window):
            for match in pattern.finditer(window):
                page_id = match.group(1) or match.group(2)
                if page_id and page_id not in business_ids:
                    return page_id
        start = index + len(target)


def _hit_inside_field(markup: str, index: int) -> bool:
    """Tên nằm trong value/placeholder của ô nhập hoặc ô tìm — chưa phải dòng Page."""
    prefix = (markup or "")[max(0, index - 280) : index]
    if re.search(r"<(?:input|textarea)\b[^>]*$", prefix, re.I | re.S):
        return True
    return bool(re.search(r"(?:placeholder|aria-label|value)\s*=\s*[\"'][^\"']*$", prefix, re.I))


def _name_hits_outside_fields(markup: str, target: str) -> bool:
    """True khi tên Page xuất hiện ngoài ô tìm / ô nhập."""
    needle = " ".join((target or "").split()).casefold()
    if not needle:
        return False
    haystack = (markup or "").casefold()
    start = 0
    while True:
        index = haystack.find(needle, start)
        if index < 0:
            return False
        if not _hit_inside_field(markup or "", index):
            return True
        start = index + len(needle)


def _shot(page, tag: str) -> None:
    folder = project_root() / "logs" / "screenshots"
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"page_create_{tag}_{int(time.time())}.png"
    try:
        page.screenshot(path=str(path))
        logger.info("Đã chụp màn hình tạo Page: {}", path)
    except Exception as exc:  # noqa: BLE001
        logger.debug("Không chụp được màn hình tạo Page: {}", exc)


def _human_pause(page) -> None:
    page.wait_for_timeout(random.randint(1000, 3000))


def _step_pause(page) -> None:
    """Chờ sau mỗi lần gõ hoặc chọn để Facebook ghi nhận trước bước tiếp theo."""
    try:
        page.wait_for_timeout(random.randint(1400, 2400))
    except Exception:  # noqa: BLE001
        pass


class FacebookPageCreationProvider:
    """Mở profile sẵn có của account, tạo một Page, rồi xác minh tên hiện trên BM."""

    def __init__(self) -> None:
        self._sessions: dict[str, tuple[object, object, object]] = {}
        self._mu = threading.Lock()

    def begin_account(self, account_id: str) -> None:
        """Mở một trình duyệt cho cả batch. Job sau không mở lại profile."""
        self._ensure_session(account_id)

    def end_account(self, account_id: str) -> None:
        """Đóng trình duyệt khi batch hoàn tất, hủy hoặc dừng hẳn."""
        self._close_session(account_id)

    def check_session(self, account_id: str) -> PageCreationOutcome:
        """Account còn trong kho thì cho phép mở trình duyệt; tường đăng nhập xử lý lúc tạo."""
        from src.utils.db_manager import AccountsDatabaseManager

        row = AccountsDatabaseManager().get_by_id(account_id)
        if row is None:
            return PageCreationOutcome(
                ok=False,
                error_code="SESSION_EXPIRED",
                error_message="Không tìm thấy account.",
            )
        return PageCreationOutcome(ok=True)

    def verify_existing(self, request: PageCreationRequest) -> PageCreationOutcome:
        """Tìm Page cùng tên; nếu có id thì vẫn chạy pipeline avatar/About/admin."""
        local = _local_page(request)
        if local is not None and local.page_id:
            if request.should_stop():
                return PageCreationOutcome(ok=False, error_code="CANCELLED", error_message="Đã hủy")
            page_id = local.page_id
            page_url = local.page_url or f"https://www.facebook.com/{page_id}"

            def _complete_local(page, req: PageCreationRequest) -> PageCreationOutcome:
                return self._finish_created(page, req, page_id, page_url)

            return self._with_page(request, _complete_local)
        if request.should_stop():
            return PageCreationOutcome(ok=False, error_code="CANCELLED", error_message="Đã hủy")
        return self._with_page(request, self._verify_on_page)

    def create_and_verify(self, request: PageCreationRequest) -> PageCreationOutcome:
        """Điền tên, tải avatar, rồi chỉ trả thành công khi đọc được page id."""
        if request.should_stop():
            return PageCreationOutcome(ok=False, error_code="CANCELLED", error_message="Đã hủy")
        return self._with_page(request, self._create_on_page)

    def _with_page(self, request: PageCreationRequest, action) -> PageCreationOutcome:
        from src.automation.browser_factory import BrowserFactory, prepare_playwright_sync_thread, sync_close_persistent_context
        from src.utils.account_proxy_mapper import prepare_account_dict_for_browser_run
        from src.utils.db_manager import AccountsDatabaseManager

        row = AccountsDatabaseManager().get_by_id(request.account_id)
        if row is None:
            return PageCreationOutcome(ok=False, error_code="SESSION_EXPIRED", error_message="Không tìm thấy account.")
        borrowed = self._borrow_page(request.account_id)
        if borrowed is not None:
            try:
                return action(borrowed, request)
            except Exception as exc:  # noqa: BLE001
                return _outcome_from_exception(request.page_name, exc)
        factory = None
        context = None
        try:
            prepare_playwright_sync_thread(label=f"page-create:{request.account_id}")
            acc = prepare_account_dict_for_browser_run(dict(row), require_proxy_live=True)
            factory = BrowserFactory(headless=False, playwright_shared=True)
            context = factory.launch_persistent_context_from_account_dict(
                acc,
                headless=False,
                disable_notifications=True,
                force_desktop_facebook=True,
            )
            page = context.pages[0] if context.pages else context.new_page()
            return action(page, request)
        except Exception as exc:  # noqa: BLE001
            return _outcome_from_exception(request.page_name, exc)
        finally:
            sync_close_persistent_context(context, log_label="page-create", same_thread=True)
            if factory is not None:
                try:
                    factory.close()
                except Exception:  # noqa: BLE001
                    pass

    def _ensure_session(self, account_id: str) -> None:
        if self._borrow_page(account_id) is not None:
            return
        from src.automation.browser_factory import BrowserFactory, prepare_playwright_sync_thread
        from src.utils.account_proxy_mapper import prepare_account_dict_for_browser_run
        from src.utils.db_manager import AccountsDatabaseManager

        row = AccountsDatabaseManager().get_by_id(account_id)
        if row is None:
            raise RuntimeError("Không tìm thấy account.")
        prepare_playwright_sync_thread(label=f"page-create:{account_id}")
        acc = prepare_account_dict_for_browser_run(dict(row), require_proxy_live=True)
        factory = BrowserFactory(headless=False, playwright_shared=True)
        context = factory.launch_persistent_context_from_account_dict(
            acc,
            headless=False,
            disable_notifications=True,
            force_desktop_facebook=True,
        )
        page = context.pages[0] if context.pages else context.new_page()
        with self._mu:
            self._sessions[account_id] = (factory, context, page)

    def _borrow_page(self, account_id: str):
        with self._mu:
            item = self._sessions.get(account_id)
        if item is None:
            return None
        _factory, _context, page = item
        try:
            if page.is_closed():
                self._close_session(account_id)
                return None
        except Exception:  # noqa: BLE001
            self._close_session(account_id)
            return None
        return page

    def _close_session(self, account_id: str) -> None:
        from src.automation.browser_factory import sync_close_persistent_context

        with self._mu:
            item = self._sessions.pop(account_id, None)
        if item is None:
            return
        factory, context, _page = item
        sync_close_persistent_context(context, log_label="page-create", same_thread=True)
        try:
            factory.close()
        except Exception:  # noqa: BLE001
            pass

    def _open_business_pages(self, page, request: PageCreationRequest) -> str:
        bid = request.business_id.strip()
        url = f"https://business.facebook.com/latest/settings/pages?business_id={bid}"
        page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        _human_pause(page)
        try:
            body = page.inner_text("body")
        except Exception:  # noqa: BLE001
            body = ""
        return classify_facebook_surface(page.url, body)

    def _open_profile_pages(self, page, request: PageCreationRequest) -> str:
        """Mở trang tạo Page trên profile cá nhân, không qua Business Manager."""
        urls = (
            "https://www.facebook.com/pages/create",
            "https://www.facebook.com/pages/?category=your_pages&ref=bookmarks",
        )
        last_code = ""
        for url in urls:
            page.goto(url, wait_until="domcontentloaded", timeout=60_000)
            _human_pause(page)
            try:
                body = page.inner_text("body")
            except Exception:  # noqa: BLE001
                body = ""
            last_code = classify_facebook_surface(page.url, body)
            if last_code:
                return last_code
            if "pages/create" in (page.url or "").lower():
                return ""
            if _open_create_form(page)[0]:
                return ""
        return last_code

    def _open_create_surface(self, page, request: PageCreationRequest) -> str:
        mode = (request.create_mode or "bm").strip().casefold()
        if mode == "profile":
            return self._open_profile_pages(page, request)
        if not request.business_id.strip():
            return "NAVIGATION_TIMEOUT"
        return self._open_business_pages(page, request)

    def probe_page_creation_gate(self, account_id: str, business_id: str, create_mode: str = "bm") -> PageCreationOutcome:
        """Kiểm tra lại sau khi user xử lý SMS/CAPTCHA. Không tạo Page."""
        request = PageCreationRequest(
            job_id="probe",
            batch_id="probe",
            account_id=account_id,
            business_id=business_id,
            page_name="",
            avatar_path="",
            create_mode=create_mode or "bm",
        )
        return self._with_page(request, self._probe_on_page)

    def _probe_on_page(self, page, request: PageCreationRequest) -> PageCreationOutcome:
        code = self._open_create_surface(page, request)
        if code:
            _shot(page, f"probe_{code.lower()}")
            return PageCreationOutcome(
                ok=False,
                error_code=code,
                error_message=user_message_for_code(code),
            )
        return PageCreationOutcome(ok=True)

    def _verify_on_page(self, page, request: PageCreationRequest) -> PageCreationOutcome:
        code = self._open_create_surface(page, request)
        if code:
            _shot(page, code.lower())
            return PageCreationOutcome(
                ok=False,
                error_code=code,
                error_message=user_message_for_code(code),
            )
        page_id, page_url = _find_named_page(page, request.page_name)
        if not page_id:
            page_id, page_url = _search_pages_list(page, request.page_name)
        if not page_id and (request.create_mode or "").casefold() == "profile":
            page_id, page_url = _find_named_page_on_profile(page, request.page_name, allow_navigate=True)
        if not page_id:
            return PageCreationOutcome(ok=False, error_code="", error_message="Chưa thấy Page trên tài khoản.")
        return self._finish_created(page, request, page_id, page_url)

    def _create_on_page(self, page, request: PageCreationRequest) -> PageCreationOutcome:
        if request.should_stop():
            return PageCreationOutcome(ok=False, error_code="CANCELLED", error_message="Đã hủy")
        _notify_progress(request, "Mở trang tạo Page", step=1, state="CREATING")
        code = self._open_create_surface(page, request)
        if code:
            _shot(page, code.lower())
            return PageCreationOutcome(
                ok=False,
                error_code=code,
                error_message=user_message_for_code(code),
            )
        existing_id, existing_url = _find_named_page(page, request.page_name)
        if not existing_id:
            existing_id, existing_url = _search_pages_list(page, request.page_name)
        if existing_id:
            _notify_progress(
                request,
                f"Đã có Page «{request.page_name}» — UID {existing_id} · {existing_url}. Không tạo lại.",
                step=6,
            )
            return self._finish_created(page, request, existing_id, existing_url)

        _dismiss_invite(page)
        _notify_progress(request, "Mở form Tạo Trang mới", step=2, state="CREATING")
        opened, seen = _open_create_form(page)
        if not opened and (request.create_mode or "").casefold() == "profile":
            # Form create đã mở sẵn trên /pages/create
            opened = _profile_create_form_visible(page)
            seen = seen or _visible_labels(page)
        if not opened:
            _shot(page, "navigation_timeout")
            shown = ", ".join(seen[:8]) or "không thấy nút nào"
            return PageCreationOutcome(
                ok=False,
                error_code="NAVIGATION_TIMEOUT",
                error_message=f"Không thấy nút Tạo Trang mới. Nút trên màn hình: {shown}.",
            )
        _human_pause(page)
        blocked = _surface_code(page)
        if blocked:
            _shot(page, blocked.lower())
            return PageCreationOutcome(
                ok=False,
                error_code=blocked,
                error_message=user_message_for_code(blocked),
            )
        _step_pause(page)
        from .naming import name_repair_candidates, sanitize_page_name

        _notify_progress(request, f"Điền tên «{request.page_name}»", step=3, state="CREATING")
        preferred = sanitize_page_name(request.page_name) or request.page_name.strip()
        accepted_name = _resolve_page_name(page, preferred, extras=name_repair_candidates(request.page_name))
        if not accepted_name:
            _shot(page, "name_invalid")
            return PageCreationOutcome(
                ok=False,
                error_code="TEMPORARY_ERROR",
                error_message=(
                    f"Tên Page «{request.page_name}» bị Facebook từ chối và không sửa được tự động. "
                    "Hãy đổi tên rồi chạy lại."
                ),
            )
        if accepted_name != request.page_name.strip():
            logger.info("Đổi tên Page «{}» → «{}» để vượt kiểm tra tên.", request.page_name, accepted_name)
            request.page_name = accepted_name
            _notify_progress(request, f"Đã sửa tên thành «{accepted_name}»", step=3)
        _refresh_request_details(request)
        _step_pause(page)
        category = (request.category or "").strip() or _default_category_for_request(
            request, english=_form_is_english(page)
        )
        category = _normalize_category_choice(category, english=_form_is_english(page))
        request.category = category
        _notify_progress(request, f"Chọn hạng mục «{category}»", step=4, state="CREATING")
        # Một lần chọn. Chip đã có thì hàm tự dừng, không gõ Product/service đè lên.
        category_ok = _fill_category_if_present(page, category)
        if category_ok and _committed_category_labels(page):
            request.category = _committed_category_labels(page)[0]
        if not category_ok or not _category_chip_matches(page, category):
            logger.warning("Chưa chọn đúng hạng mục «{}» cho «{}».", category, request.page_name)
            _shot(page, "category_miss")
            return PageCreationOutcome(
                ok=False,
                error_code="TEMPORARY_ERROR",
                error_message=f"Chưa chọn đúng hạng mục «{category}». Chưa bấm Next.",
            )
        _release_category_for_next(page)
        _step_pause(page)
        bio_text = request.bio.strip()
        _notify_progress(request, "Điền Bio / mô tả", step=5, state="FILLING_DETAILS")
        bio_ok = False
        for _bio_try in range(3):
            _release_category_for_next(page)
            if _fill_bio_if_present(page, bio_text):
                bio_ok = True
                break
            _step_pause(page)
        if bio_text and not bio_ok:
            logger.warning("Chưa điền được Bio cho «{}» — không bấm Next.", request.page_name)
            _shot(page, "bio_miss")
            return PageCreationOutcome(
                ok=False,
                error_code="TEMPORARY_ERROR",
                error_message="Chưa điền được Bio. Nút Next còn khóa. Chưa bấm Next.",
            )
        _release_category_for_next(page)
        if request.should_stop():
            return PageCreationOutcome(ok=False, error_code="CANCELLED", error_message="Đã hủy")
        _notify_progress(request, "Bấm Next", step=6, state="CREATING")
        _step_pause(page)
        if not _leave_details_with_next(page):
            _shot(page, "navigation_timeout")
            return PageCreationOutcome(
                ok=False,
                error_code="NAVIGATION_TIMEOUT",
                error_message="Đã điền tên, hạng mục và mô tả nhưng chưa bấm được Next.",
            )
        _notify_progress(request, "Tick điều khoản và bấm Tạo Trang", step=7, state="CREATING")
        _step_pause(page)
        if not _tick_and_create_page(page):
            _shot(page, "navigation_timeout")
            return PageCreationOutcome(
                ok=False,
                error_code="NAVIGATION_TIMEOUT",
                error_message="Chưa tick điều khoản hoặc chưa bấm được Tạo Trang.",
            )
        _notify_progress(request, "Lưu UID và link Page", step=8, state="VERIFYING")
        page_id, page_url = _capture_created_page(page, request)
        if not page_id:
            _shot(page, "verify_missing")
            return PageCreationOutcome(
                ok=False,
                error_code="NAVIGATION_TIMEOUT",
                error_message="Đã bấm Tạo Trang nhưng chưa đọc được UID và link. Không tạo trùng ngay.",
            )
        _notify_progress(request, f"Đã lưu UID {page_id} · {page_url}", step=8)
        return self._finish_created(page, request, page_id, page_url)

    def _finish_created(self, page, request: PageCreationRequest, page_id: str, page_url: str) -> PageCreationOutcome:
        """Page đã có id: luôn chạy pipeline đầy đủ; chỉ ok khi thông tin đủ."""
        _notify_progress(
            request,
            f"UID {page_id} đã lưu — thêm ảnh đại diện, SĐT, email",
            step=9,
            state="FILLING_DETAILS",
        )
        summary = ensure_page_complete(page, request, page_id)
        complete, reason = _pipeline_is_complete(request, summary)
        page_url_final = page_url or f"https://www.facebook.com/{page_id}"
        # Đã có UID thì khóa trang này và chuyển Page sau — không mở lại form tạo.
        if not complete:
            _notify_progress(
                request,
                f"Đã tạo — UID {page_id} · {page_url_final}. Còn thiếu: {reason}. Chuyển Page tiếp theo.",
                step=10,
            )
        else:
            _notify_progress(
                request,
                f"Hoàn tất — UID {page_id} · {page_url_final}",
                step=10,
            )
        return PageCreationOutcome(
            ok=True,
            page_id=page_id,
            page_url=page_url_final,
            page_name=request.page_name.strip(),
        )


def _wanted_contact_count(request: PageCreationRequest) -> int:
    """Số trường liên hệ/bio cần điền theo request."""
    return sum(
        1
        for key in ("phone", "email", "website", "address", "bio")
        if str(getattr(request, key, "") or "").strip()
    )


def _pipeline_is_complete(request: PageCreationRequest, summary: dict[str, object]) -> tuple[bool, str]:
    """
    Kiểm tra pipeline hậu tạo đã đủ chưa.

    Bắt buộc: avatar nếu có file; đủ (hoặc gần đủ) trường liên hệ đã chuẩn bị;
    admin nếu có danh sách target.
    """
    missing: list[str] = []
    avatar = str(request.avatar_path or "").strip()
    need_avatar = bool(avatar and Path(avatar).is_file())
    if need_avatar and not bool(summary.get("avatar")):
        missing.append("ảnh đại diện")

    wanted = _wanted_contact_count(request)
    filled = int(summary.get("details_filled") or 0)
    # Chấp nhận nếu điền được ít nhất 3 trường hoặc toàn bộ khi ít hơn 3.
    need_details = min(wanted, 3) if wanted else 0
    if need_details and filled < need_details:
        missing.append(f"thông tin liên hệ ({filled}/{need_details})")

    targets = [str(item).strip() for item in (request.admin_targets or []) if str(item).strip()]
    if targets and int(summary.get("admins_added") or 0) <= 0:
        missing.append("quản trị viên")

    if missing:
        return False, ", ".join(missing)
    return True, ""


def _page_switch_prompt_visible(page) -> bool:
    try:
        body = page.inner_text("body")
    except Exception:  # noqa: BLE001
        body = ""
    return page_switch_prompt_text(body)


def _switch_into_created_page(page) -> bool:
    """Bấm Switch Now rồi Switch xác nhận để vào Page trước khi sửa thông tin."""
    if not _page_switch_prompt_visible(page):
        return True
    for _try in range(3):
        if not _page_switch_prompt_visible(page):
            return True
        clicked = _click_matching(page, is_switch_now_label, roles=("button", "link"))
        if not clicked:
            clicked = _click_matching(page, is_page_switch_label, roles=("button",))
        if not clicked:
            break
        _human_pause(page)
        try:
            page.wait_for_load_state("domcontentloaded", timeout=15_000)
        except Exception:  # noqa: BLE001
            pass
        _human_pause(page)
    switched = not _page_switch_prompt_visible(page)
    if not switched:
        _shot(page, "switch_miss")
    return switched


def ensure_page_complete(page, request: PageCreationRequest, page_id: str) -> dict[str, object]:
    """
    Pipeline hậu tạo — luôn chạy trên mọi nhánh thành công (kể cả Page có sẵn).

    Thứ tự: Finish setting (nếu còn) → avatar → About (contact) → admins.
    Không vượt CAPTCHA/checkpoint. Không đóng wizard khi chưa điền xong.
    """
    pid = (page_id or "").strip()
    summary: dict[str, object] = {
        "avatar": False,
        "details_filled": 0,
        "admins_added": 0,
        "notes": [],
        "complete": False,
    }
    if not pid:
        return summary
    if _creation_confirm_open(page):
        summary["notes"] = ["confirm_dialog_open"]
        logger.info("Chưa tạo Page — hộp đồng ý điều khoản vẫn mở, không gắn ảnh.")
        return summary

    _refresh_request_details(request)
    notes: list[str] = []

    if _page_switch_prompt_visible(page):
        _notify_progress(request, "Bấm Switch để vào Page", step=9, state="FILLING_DETAILS")
        if _switch_into_created_page(page):
            notes.append("switch:ok")
        else:
            logger.warning("Page {} đang mở nhưng chưa bấm được Switch.", pid)
            notes.append("switch:miss")

    # Ưu tiên wizard Finish setting còn mở — đừng goto About (sẽ tắt bước cài đặt).
    if finish_setting_visible(page):
        _notify_progress(request, "Thêm ảnh và thông tin trên wizard", step=9, state="FILLING_DETAILS")
        wizard = _complete_finish_setting_wizard(page, request)
        summary["avatar"] = bool(wizard.get("avatar"))
        summary["details_filled"] = int(wizard.get("details_filled") or 0)
        notes.extend(str(item) for item in (wizard.get("notes") or []))
        _step_pause(page)

    avatar = str(request.avatar_path or "").strip()
    if avatar and Path(avatar).is_file() and not summary["avatar"]:
        _notify_progress(request, "Gắn ảnh đại diện", step=8, state="UPLOADING_AVATAR")
        uploaded = False
        if finish_setting_visible(page):
            uploaded = _handle_post_create_avatar(page, avatar)
        if not uploaded:
            uploaded = _upload_avatar_on_page_profile(page, pid, avatar)
        summary["avatar"] = bool(uploaded)
        if uploaded:
            logger.info("Page {}: đã gắn ảnh đại diện.", pid)
            notes.append("avatar:ok")
        else:
            logger.warning("Page {}: chưa gắn được ảnh đại diện từ {}.", pid, Path(avatar).name)
            _shot(page, "avatar_miss")
            notes.append("avatar:miss")
        _step_pause(page)
    elif not avatar:
        notes.append("avatar:skip")

    contact = {
        "phone": request.phone,
        "email": request.email,
        "website": request.website,
        "address": request.address,
        "bio": request.bio,
    }
    # About chỉ khi wizard đã xong — tránh page.goto làm tắt Finish setting.
    if any(str(value or "").strip() for value in contact.values()) and not finish_setting_visible(page):
        _notify_progress(request, "Điền thông tin About (SĐT, email, website…)", step=9, state="FILLING_DETAILS")
        filled, detail = _fill_page_contact_details(page, pid, contact)
        already = int(summary.get("details_filled") or 0)
        summary["details_filled"] = already + int(filled)
        if detail in {
            "CAPTCHA_DETECTED",
            "CHECKPOINT",
            "SESSION_EXPIRED",
            "RATE_LIMITED",
            "SMS_VERIFICATION_REQUIRED",
        }:
            logger.warning(
                "Page {} đã tạo nhưng bị chặn khi điền thông tin ({}). Page giữ nguyên.",
                pid,
                detail,
            )
            notes.append(f"details:{detail}")
        elif detail and filled <= 0 and already <= 0:
            logger.warning("Page {} đã tạo nhưng chưa điền được thông tin: {}", pid, detail)
            _shot(page, "details_miss")
            notes.append("details:miss")
        elif detail:
            logger.info("Page {}: {}", pid, detail)
            notes.append(f"details:{filled}")

        # Thử lại một lần nếu còn thiếu trường bắt buộc.
        need = min(_wanted_contact_count(request), 3)
        if need and int(summary["details_filled"]) < need and not finish_setting_visible(page):
            _notify_progress(request, "Thử điền lại thông tin còn thiếu", step=9, state="FILLING_DETAILS")
            filled2, detail2 = _fill_page_contact_details(page, pid, contact)
            summary["details_filled"] = int(summary["details_filled"]) + int(filled2)
            if detail2:
                notes.append(f"details_retry:{filled2}")
            _step_pause(page)
    elif finish_setting_visible(page):
        notes.append("details:wizard_still_open")
    else:
        notes.append("details:empty")

    targets = [str(item).strip() for item in (request.admin_targets or []) if str(item).strip()]
    if targets and not finish_setting_visible(page):
        _notify_progress(request, f"Thêm quản trị viên ({len(targets)})", step=10, state="ADDING_ADMIN")
        from .page_admin import add_admins_on_page

        added, detail = add_admins_on_page(page, pid, targets)
        summary["admins_added"] = int(added)
        if detail in {
            "CAPTCHA_DETECTED",
            "CHECKPOINT",
            "SESSION_EXPIRED",
            "RATE_LIMITED",
            "SMS_VERIFICATION_REQUIRED",
        }:
            logger.warning(
                "Page {} đã tạo nhưng bị chặn khi thêm quản trị viên ({}). Page giữ nguyên, không tạo lại.",
                pid,
                detail,
            )
            notes.append(f"admins:{detail}")
        elif detail and added <= 0:
            logger.warning("Page {} đã tạo nhưng chưa thêm được quản trị viên: {}", pid, detail)
            _shot(page, "admins_miss")
            notes.append("admins:miss")
        elif detail:
            logger.info("Page {}: {}", pid, detail)
            notes.append(f"admins:{added}")
    elif not targets:
        notes.append("admins:skip")

    complete, reason = _pipeline_is_complete(request, summary)
    summary["complete"] = complete
    summary["incomplete_reason"] = reason
    summary["notes"] = notes
    if complete:
        _notify_progress(request, "Đã điền đủ ảnh + thông tin Page", step=10)
    else:
        _notify_progress(request, f"Còn thiếu: {reason}", step=10)
    return summary


def _refresh_request_details(request: PageCreationRequest) -> None:
    """Sinh lại bio/phone/email/… nếu job để trống nhưng đã có tên Page."""
    from .page_details import fill_missing_details

    lang = "en" if not request.page_name else None
    filled = fill_missing_details(
        request.page_name,
        {
            "category": request.category,
            "bio": request.bio,
            "phone": request.phone,
            "email": request.email,
            "website": request.website,
            "address": request.address,
        },
        language=lang,
    )
    request.category = filled.get("category", "") or request.category
    request.bio = filled.get("bio", "") or request.bio
    request.phone = filled.get("phone", "") or request.phone
    request.email = filled.get("email", "") or request.email
    request.website = filled.get("website", "") or request.website
    request.address = filled.get("address", "") or request.address


def _prepare_create_form(page, request: PageCreationRequest) -> bool:
    """
    Chuẩn bị form trước Next/Create: đủ 1 category, có bio, tên OK, điều khoản.

    Không chọn lại category nếu đã có đúng 1 chip — tránh chồng Arts/Product/Community/Brand.
    """
    category = (request.category or "").strip()
    if _category_box(page) is not None:
        # Chip đã có (kể cả khi ô còn gõ lại đúng tên chip) → xóa chữ thừa, không chọn lại.
        if _category_already_chosen(page):
            if len(_committed_category_labels(page)) > 1:
                _keep_only_one_category_chip(page, category)
            _release_category_for_next(page)
        elif not _fill_category_if_present(page, category):
            logger.warning("Hạng mục chưa chọn trước Create cho «{}» — tạm hoãn bấm.", request.page_name)
            _shot(page, "category_miss_pre_create")
            return False
        else:
            _release_category_for_next(page)

    bio_text = (request.bio or "").strip()
    if bio_text:
        _focus_bio_field(page)
        for _try in range(2):
            if _fill_bio_if_present(page, bio_text):
                break
            _step_pause(page)
        else:
            logger.warning("Bio vẫn trống trước Create cho «{}».", request.page_name)
            _shot(page, "bio_miss_pre_create")

    if _name_field_invalid(page):
        logger.warning("Tên Page vẫn invalid trước Create — không bấm Tạo Trang.")
        _shot(page, "name_invalid_pre_create")
        return False

    if not _accept_creation_terms(page):
        return False
    return True


def _local_page(request: PageCreationRequest) -> PageCreationOutcome | None:
    """Page đã lưu local với cùng account, tên (và BM nếu đang tạo qua BM)."""
    name = request.page_name.strip().casefold()
    mode = (request.create_mode or "bm").strip().casefold()
    try:
        from src.utils.pages_manager import PagesManager

        for row in PagesManager().load_all():
            if str(row.get("account_id") or "") != request.account_id:
                continue
            if mode == "bm" and str(row.get("business_id") or "") != request.business_id:
                continue
            if str(row.get("page_name") or "").strip().casefold() != name:
                continue
            page_id = str(row.get("fb_page_id") or "").strip()
            page_url = str(row.get("page_url") or "").strip()
            if page_id:
                return PageCreationOutcome(ok=True, page_id=page_id, page_url=page_url)
    except Exception as exc:  # noqa: BLE001
        logger.debug("Đọc pages.json khi xác minh Page: {}", exc)
    return None


def _outcome_from_exception(page_name: str, exc: Exception) -> PageCreationOutcome:
    logger.warning("Tạo Page không xong ({}): {}", page_name, exc)
    message = str(exc)
    code = "NETWORK_TIMEOUT" if "timeout" in message.lower() or "proxy" in message.lower() else "TEMPORARY_ERROR"
    return PageCreationOutcome(ok=False, error_code=code, error_message=message)


def _surface_code(page) -> str:
    try:
        body = page.inner_text("body")
    except Exception:  # noqa: BLE001
        body = ""
    try:
        url = page.url
    except Exception:  # noqa: BLE001
        url = ""
    return classify_facebook_surface(url, body)


def _page_markup(page) -> str:
    parts: list[str] = []
    try:
        parts.append(page.url or "")
    except Exception:  # noqa: BLE001
        pass
    try:
        parts.append(page.content() or "")
    except Exception:  # noqa: BLE001
        pass
    try:
        parts.append(page.inner_text("body") or "")
    except Exception:  # noqa: BLE001
        pass
    return "\n".join(parts)


def _dismiss_invite(page) -> None:
    from .business_creator import _dismiss_invite_dialog, is_invite_friends_dialog

    try:
        dialog = page.get_by_role("dialog")
        text = dialog.first.inner_text(timeout=1_000) if dialog.count() else ""
    except Exception:  # noqa: BLE001
        text = ""
    if is_invite_friends_dialog(text):
        _dismiss_invite_dialog(page)


def _iter_roots(page):
    """Trang chính và mọi frame. Business Suite thường để nút tạo Page trong iframe."""
    yield page
    try:
        frames = list(page.frames)
        main = page.main_frame
    except Exception:  # noqa: BLE001
        return
    for frame in frames:
        if frame == main:
            continue
        yield frame


def _open_create_form(page) -> tuple[bool, list[str]]:
    """Bấm «+ Add» rồi «Create a new Page». Không dừng ở ô tìm của danh sách."""
    deadline = time.time() + 12
    seen: list[str] = []
    add_clicks = 0
    menu_roles = ("button", "menuitem", "link", "option")
    while time.time() < deadline:
        seen = _visible_labels(page)
        if _click_matching(page, is_create_page_label, roles=menu_roles):
            return True, seen
        if add_clicks >= 2:
            page.wait_for_timeout(700)
            continue
        if not _click_matching(page, is_add_menu_label, roles=("button",)):
            page.wait_for_timeout(700)
            continue
        add_clicks += 1
        _human_pause(page)
        item_deadline = time.time() + 3
        while time.time() < item_deadline:
            seen = _visible_labels(page)
            if _click_matching(page, is_create_page_label, roles=menu_roles):
                return True, seen
            page.wait_for_timeout(400)
        logger.info("Đã bấm + Add nhưng chưa thấy mục Tạo Trang mới. Nút: {}", ", ".join(seen[:8]))
    return False, seen or _visible_labels(page)


def _visible_labels(page) -> list[str]:
    found: list[str] = []
    for root in _iter_roots(page):
        for role in ("button", "link", "menuitem"):
            try:
                loc = root.get_by_role(role)
                count = min(loc.count(), 20)
            except Exception:  # noqa: BLE001
                continue
            for index in range(count):
                try:
                    item = loc.nth(index)
                    if not item.is_visible():
                        continue
                    text = _first_line(item.inner_text(timeout=400))
                except Exception:  # noqa: BLE001
                    continue
                if text and text not in found:
                    found.append(text)
                if len(found) >= 12:
                    return found
    return found


_PAGE_LIST_SEARCH = re.compile(r"search by name or id|tìm theo tên hoặc id|search by name", re.I)


def _search_pages_list(page, page_name: str) -> tuple[str, str]:
    """Gõ tên vào ô tìm của danh sách Page. Có rồi thì trả UID, không mở form tạo."""
    name = " ".join((page_name or "").split())
    if not name:
        return "", ""
    box = None
    for root in _iter_roots(page):
        try:
            loc = root.get_by_placeholder(_PAGE_LIST_SEARCH)
            if loc.count() and loc.first.is_visible():
                box = loc.first
                break
        except Exception:  # noqa: BLE001
            pass
        try:
            loc = root.get_by_role("searchbox", name=_PAGE_LIST_SEARCH)
            if loc.count() and loc.first.is_visible():
                box = loc.first
                break
        except Exception:  # noqa: BLE001
            continue
    if box is None:
        return "", ""
    try:
        box.click(timeout=3_000)
        box.press("Control+A")
        box.press("Backspace")
        page.keyboard.type(name, delay=35)
        page.wait_for_timeout(1600)
    except Exception as exc:  # noqa: BLE001
        logger.debug("Không tìm được Page «{}» trong danh sách: {}", name, exc)
        return "", ""
    found = _find_named_page(page, name)
    if found[0]:
        return found
    # Không có dòng đúng tên thì xóa ô tìm rồi để bước sau bấm + Add / Tạo Trang.
    _clear_control(box)
    return "", ""


def _find_named_page(page, page_name: str) -> tuple[str, str]:
    """Tìm Page đúng tên qua data-surface, asset_id, rồi link đúng chữ."""
    if _creation_confirm_open(page):
        return "", ""
    found = page_match_from_markup(_page_markup(page), page_name)
    if found[0]:
        return found
    try:
        link = page.get_by_role("link", name=page_name, exact=True)
        if link.count() == 0:
            return "", ""
        href = link.first.get_attribute("href") or ""
    except Exception:  # noqa: BLE001
        return "", ""
    match = re.search(r"(?<![\w])(\d{8,})", href or "")
    if not match:
        return "", ""
    page_id = match.group(1)
    url = href if href.startswith("http") else f"https://www.facebook.com/{page_id}"
    return page_id, url


def _profile_create_form_visible(page) -> bool:
    """Form tạo Page profile đã hiện ô tên."""
    try:
        box = page.get_by_role("textbox", name=_PAGE_NAME_FIELD)
        return box.count() > 0 and box.first.is_visible()
    except Exception:  # noqa: BLE001
        return False


def _find_named_page_on_profile(page, page_name: str, *, allow_navigate: bool = True) -> tuple[str, str]:
    """Đọc Page trên profile. Chỉ sang Your Pages khi allow_navigate=True (sau khi wizard xong)."""
    found = _find_named_page(page, page_name)
    if found[0]:
        return found
    try:
        current = page.url or ""
    except Exception:  # noqa: BLE001
        current = ""
    match = re.search(r"(?:facebook\.com/(?:profile\.php\?id=)?|(?:page_id|id)=)(\d{8,})", current, re.I)
    markup = _page_markup(page)
    if match and page_name.casefold() in (markup or "").casefold():
        page_id = match.group(1)
        return page_id, f"https://www.facebook.com/{page_id}"
    # URL vừa redirect về Page mới: /PageName-123 hoặc /123456789
    if match and ("pages/create" not in current.casefold()) and ("dialog" not in (markup or "").casefold()):
        # Chỉ nhận khi tên đã hết form tạo hoặc URL chứa tên
        if page_name.casefold().replace(" ", "") in current.casefold().replace(" ", "") or "your_pages" in current.casefold():
            page_id = match.group(1)
            return page_id, f"https://www.facebook.com/{page_id}"
    if not allow_navigate:
        return "", ""
    try:
        page.goto(
            "https://www.facebook.com/pages/?category=your_pages&ref=bookmarks",
            wait_until="domcontentloaded",
            timeout=60_000,
        )
        _human_pause(page)
    except Exception as exc:  # noqa: BLE001
        logger.debug("Không mở được Your Pages: {}", exc)
        return "", ""
    return _find_named_page(page, page_name)


def _wait_for_profile_page(page, page_name: str) -> tuple[str, str]:
    """Chờ id hiện trên trang hiện tại. Không rời wizard giữa chừng."""
    for _wait in range(8):
        found = _find_named_page_on_profile(page, page_name, allow_navigate=False)
        if found[0]:
            return found
        try:
            page.wait_for_timeout(1000)
        except Exception:  # noqa: BLE001
            break
    return _find_named_page_on_profile(page, page_name, allow_navigate=True)


def _click_matching(page, predicate, *, roles: tuple[str, ...] = ("button", "link", "menuitem")) -> bool:
    for root in _iter_roots(page):
        if _click_in(root, predicate, roles):
            return True
    return False


def _click_in(root, predicate, roles: tuple[str, ...]) -> bool:
    targets = [root]
    try:
        dialog = root.get_by_role("dialog")
        if dialog.count() and dialog.first.is_visible():
            targets.insert(0, dialog.first)
    except Exception:  # noqa: BLE001
        pass
    for target in targets:
        if _click_roles(target, predicate, roles):
            return True
    return False


def _click_roles(root, predicate, roles: tuple[str, ...]) -> bool:
    for role in roles:
        try:
            loc = root.get_by_role(role)
            count = min(loc.count(), 40)
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


def _fill_page_name(page, page_name: str) -> bool:
    """Gõ tên vào ô Page name. True nếu ô giữ đúng chữ vừa nhập."""
    try:
        box = page.get_by_role("textbox", name=_PAGE_NAME_FIELD)
        if box.count() == 0:
            return False
        target = box.first
        label = " ".join(
            part
            for part in (
                target.get_attribute("aria-label") or "",
                target.get_attribute("placeholder") or "",
            )
            if part
        )
        if label and not is_page_name_field(label):
            return False
        target.click(timeout=8_000)
        _human_pause(page)
        try:
            target.press("Control+A")
        except Exception:  # noqa: BLE001
            pass
        target.type(page_name, delay=random.randint(70, 140))
        _step_pause(page)
        value = _control_value(target)
        if name_was_entered(value, page_name):
            return True
        try:
            target.click(timeout=4_000)
            target.press("Control+A")
            target.press("Backspace")
        except Exception:  # noqa: BLE001
            pass
        _step_pause(page)
        target.type(page_name, delay=random.randint(80, 150))
        _step_pause(page)
        return name_was_entered(_control_value(target), page_name)
    except Exception as exc:  # noqa: BLE001
        logger.debug("Không điền được tên Page: {}", exc)
        return False


def _page_name_feedback_text(page) -> str:
    """Lấy text cảnh báo quanh ô tên Page."""
    chunks: list[str] = []
    for root in _iter_roots(page):
        try:
            alerts = root.locator('[role="alert"], [aria-live="polite"], [aria-live="assertive"]')
            count = min(alerts.count(), 8)
        except Exception:  # noqa: BLE001
            count = 0
        for index in range(count):
            try:
                text = alerts.nth(index).inner_text(timeout=800) or ""
            except Exception:  # noqa: BLE001
                continue
            if text.strip():
                chunks.append(text)
        try:
            tips = root.get_by_text(
                re.compile(r"is invalid|không hợp lệ|suggested|đề xuất|Learn more|Tìm hiểu thêm", re.I)
            )
            tip_count = min(tips.count(), 6)
        except Exception:  # noqa: BLE001
            tip_count = 0
        for index in range(tip_count):
            try:
                text = tips.nth(index).inner_text(timeout=800) or ""
            except Exception:  # noqa: BLE001
                continue
            if text.strip():
                chunks.append(text)
    return "\n".join(chunks)


def _name_field_invalid(page) -> bool:
    return name_feedback_is_invalid(_page_name_feedback_text(page))


def _click_suggested_name_if_present(page) -> str:
    """Bấm link gợi ý tên nếu có; trả về tên đã đọc được."""
    blob = _page_name_feedback_text(page)
    suggested = parse_suggested_page_name(blob)
    if not suggested:
        return ""
    pattern = re.compile(re.escape(suggested))
    for root in _iter_roots(page):
        for role in ("link", "button"):
            try:
                loc = root.get_by_role(role, name=pattern)
                if loc.count() and loc.first.is_visible():
                    loc.first.click(timeout=4_000)
                    _step_pause(page)
                    return suggested
            except Exception:  # noqa: BLE001
                continue
        try:
            loc = root.get_by_text(pattern)
            if loc.count() and loc.first.is_visible():
                loc.first.click(timeout=4_000)
                _step_pause(page)
                return suggested
        except Exception:  # noqa: BLE001
            continue
    return suggested


def _resolve_page_name(page, preferred: str, *, extras: list[str] | None = None) -> str:
    """
    Điền tên và chủ động sửa khi Facebook báo invalid.

    Thứ tự: tên ưu tiên → gợi ý FB → các ứng viên thay thế.
    """
    candidates: list[str] = []
    for item in [preferred, *(extras or [])]:
        text = " ".join((item or "").split())
        if text and text not in candidates:
            candidates.append(text)
    if not candidates:
        return ""
    for candidate in candidates:
        if not _fill_page_name(page, candidate):
            continue
        _step_pause(page)
        if not _name_field_invalid(page):
            return candidate
        suggested = _click_suggested_name_if_present(page)
        if suggested:
            box = _name_textbox(page)
            if box is not None and name_was_entered(_control_value(box), suggested) and not _name_field_invalid(page):
                return suggested
            if _fill_page_name(page, suggested) and not _name_field_invalid(page):
                return suggested
            if suggested not in candidates:
                candidates.append(suggested)
    return ""


def _name_textbox(page):
    try:
        box = page.get_by_role("textbox", name=_PAGE_NAME_FIELD)
        if box.count():
            return box.first
    except Exception:  # noqa: BLE001
        pass
    return None


def _default_category_for_request(request: PageCreationRequest, *, english: bool) -> str:
    if request.category.strip():
        return request.category.strip()
    from .page_details import pick_page_category

    return pick_page_category(request.page_name, "en" if english else "vi")


def _control_value(target) -> str:
    try:
        return target.input_value(timeout=2_000) or ""
    except Exception:  # noqa: BLE001
        pass
    try:
        return target.inner_text(timeout=1_000) or ""
    except Exception:  # noqa: BLE001
        return ""


def _category_box(page):
    pattern = re.compile(r"category|danh mục|hạng mục", re.I)
    for root in _iter_roots(page):
        try:
            box = root.get_by_role("combobox", name=pattern)
            if box.count() and box.first.is_visible():
                return box.first
        except Exception:  # noqa: BLE001
            continue
    return None


def category_query_for_form(english: bool, page_name: str = "", preferred: str = "") -> str:
    """Chọn một hạng mục cho form. Ưu tiên ``preferred``, map đúng ngôn ngữ form."""
    from .page_details import category_label_for_form, pick_page_category

    text = " ".join((preferred or "").split())
    if text:
        normalized = _normalize_category_choice(text, english=english)
        return category_label_for_form(normalized, english=english)
    picked = pick_page_category(page_name, "en" if english else "vi")
    return category_label_for_form(picked, english=english)


def _normalize_category_choice(category: str, *, english: bool) -> str:
    """Đổi mục dễ chỉ gõ chữ (Interest/Topic…) sang mục chọn được ổn định trên Facebook."""
    text = " ".join((category or "").split())
    if not text:
        return "Product/service" if english else "Sản phẩm/Dịch vụ"
    weak = {
        "interest",
        "sở thích",
        "so thich",
        "topic",
        "chủ đề",
        "chu de",
        "just for fun",
        "chỉ để vui",
        "chi de vui",
    }
    if text.casefold() in weak:
        return "Product/service" if english else "Sản phẩm/Dịch vụ"
    return text


def _form_is_english(page) -> bool:
    """Nhận ngôn ngữ từ tiêu đề form, không từ chữ đang nằm trong ô tìm."""
    if _text_visible(page, re.compile(r"How do you want to describe the Page|\bPage name\b", re.I)):
        return True
    if _text_visible(page, re.compile(r"Tên Trang|Hạng mục", re.I)):
        return False
    box = _category_box(page)
    label = _control_label(box) if box is not None else ""
    return bool(re.search(r"\bcategory\b", label, re.I))


def _text_visible(page, pattern: re.Pattern[str]) -> bool:
    for root in _iter_roots(page):
        try:
            loc = root.get_by_text(pattern)
            if loc.count() and loc.first.is_visible():
                return True
        except Exception:  # noqa: BLE001
            continue
    return False


def _clear_control(target) -> None:
    try:
        target.click(timeout=3_000)
        target.press("Control+A")
        target.press("Backspace")
    except Exception:  # noqa: BLE001
        pass


def _close_category_list(page) -> None:
    """Đóng danh sách bằng cách bấm lại ô tên. Không bấm Escape vì Escape đóng cả form."""
    if not _category_list_open(page):
        return
    for root in _iter_roots(page):
        try:
            name_box = root.get_by_role("textbox", name=_PAGE_NAME_FIELD)
            if name_box.count() and name_box.first.is_visible():
                name_box.first.click(timeout=3_000)
                _step_pause(page)
                return
        except Exception:  # noqa: BLE001
            continue


def _fill_bio_if_present(page, bio: str) -> bool:
    """Điền Bio trên form tạo. Chỉ tính xong khi chữ đã nằm trong ô, không nằm ở Category."""
    if not (bio or "").strip():
        return True
    wanted = bio.strip()[:255]
    _release_category_for_next(page)
    target = _bio_textbox(page)
    if target is None:
        return False
    try:
        target.scroll_into_view_if_needed(timeout=2_000)
    except Exception:  # noqa: BLE001
        pass
    try:
        current = " ".join(_control_value(target).split())
        if current and wanted.casefold()[:40] in current.casefold():
            return True
        target.click(timeout=4_000)
        _step_pause(page)
        if current:
            _clear_control(target)
            _step_pause(page)
        target.type(wanted, delay=random.randint(35, 80))
        _step_pause(page)
        typed = " ".join(_control_value(target).split())
        return bool(typed) and wanted.casefold()[:40] in typed.casefold()
    except Exception as exc:  # noqa: BLE001
        logger.debug("Không điền được tiểu sử: {}", exc)
        return False


def _bio_textbox(page):
    """Ô Bio / Description / «Tell people…». Bỏ ô tên Page và ô Category."""
    for root in _iter_roots(page):
        for finder in (
            lambda: root.get_by_role("textbox", name=_BIO_FIELD),
            lambda: root.get_by_placeholder(_BIO_FIELD),
        ):
            try:
                loc = finder()
                count = min(loc.count(), 6)
            except Exception:  # noqa: BLE001
                continue
            for index in range(count):
                item = loc.nth(index)
                if _bio_candidate(item):
                    return item
        try:
            areas = root.locator("textarea")
            count = min(areas.count(), 4)
        except Exception:  # noqa: BLE001
            continue
        visible = []
        for index in range(count):
            item = areas.nth(index)
            if _bio_candidate(item):
                visible.append(item)
        if len(visible) == 1:
            return visible[0]
    return None


def _bio_candidate(item) -> bool:
    try:
        if not item.is_visible():
            return False
        placeholder = item.get_attribute("placeholder") or ""
        label = f"{_control_label(item)} {placeholder}"
    except Exception:  # noqa: BLE001
        return False
    if re.search(r"page name|tên trang|tên page|category|hạng mục|danh mục", label, re.I):
        return False
    if is_bio_field_label(label):
        return True
    # Textarea không nhãn trong form tạo — chỉ nhận khi không phải ô tên/hạng mục.
    try:
        tag = (item.evaluate("el => el.tagName") or "").casefold()
    except Exception:  # noqa: BLE001
        tag = ""
    return tag == "textarea" and not label.strip()


def _fill_labeled_textbox(page, patterns: tuple[str, ...], value: str) -> bool:
    """Gõ vào ô text theo nhãn. Bỏ qua nếu đã có nội dung."""
    if not (value or "").strip():
        return False
    pattern = re.compile("|".join(patterns), re.I)
    for root in _iter_roots(page):
        try:
            box = root.get_by_role("textbox", name=pattern)
            count = min(box.count(), 6)
        except Exception:  # noqa: BLE001
            continue
        for index in range(count):
            target = box.nth(index)
            try:
                if not target.is_visible():
                    continue
                if " ".join(_control_value(target).split()):
                    return True
                target.click(timeout=4_000)
                _step_pause(page)
                target.type(value.strip(), delay=random.randint(40, 90))
                _step_pause(page)
                return True
            except Exception as exc:  # noqa: BLE001
                logger.debug("Không điền được ô «{}»: {}", patterns[0], exc)
    return False


def _page_about_urls(page_id: str) -> list[str]:
    pid = (page_id or "").strip()
    if not pid:
        return []
    return [
        f"https://www.facebook.com/{pid}/about_contact_and_basic_info",
        f"https://www.facebook.com/{pid}/about",
        f"https://www.facebook.com/profile.php?id={pid}&sk=about_contact_and_basic_info",
        f"https://www.facebook.com/{pid}/settings/?tab=profile_info",
    ]


def _fill_page_contact_details(page, page_id: str, details: dict[str, str]) -> tuple[int, str]:
    """Điền SĐT/email/website/địa chỉ/mô tả trên About sau khi tạo Page."""
    wanted = {key: str(details.get(key) or "").strip() for key in ("phone", "email", "website", "address", "bio")}
    if not any(wanted.values()):
        return 0, ""
    opened = False
    for url in _page_about_urls(page_id):
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=60_000)
            _human_pause(page)
        except Exception as exc:  # noqa: BLE001
            logger.debug("Không mở được About {}: {}", url, exc)
            continue
        try:
            body = page.inner_text("body")
        except Exception:  # noqa: BLE001
            body = ""
        blocked = classify_facebook_surface(page.url, body)
        if blocked:
            _shot(page, f"details_{blocked.lower()}")
            return 0, blocked
        _switch_into_created_page(page)
        opened = True
        break
    if not opened:
        _shot(page, "details_nav_timeout")
        return 0, "Không mở được trang About để điền thông tin."

    _click_edit_details(page)
    _step_pause(page)
    filled = 0
    notes: list[str] = []
    mapping = (
        ("phone", ("phone", "số điện thoại", "dien thoai", "mobile")),
        ("email", ("email", "e-mail", "thư điện tử", "thu dien tu")),
        ("website", ("website", "web", "trang web", "link")),
        ("address", ("address", "địa chỉ", "dia chi", "location", "vị trí")),
        ("bio", ("bio", "tiểu sử", "mô tả", "about", "giới thiệu", "intro")),
    )
    for key, patterns in mapping:
        value = wanted.get(key) or ""
        if not value:
            continue
        if _fill_labeled_textbox(page, patterns, value):
            filled += 1
            notes.append(key)
            continue
        # Nhiều About hiện nút Add … trước khi có ô nhập.
        if _click_add_detail(page, patterns) and _fill_labeled_textbox(page, patterns, value):
            filled += 1
            notes.append(key)
        else:
            notes.append(f"{key}:miss")
    _click_save_details(page)
    if filled <= 0:
        _shot(page, "details_empty")
    return filled, "Đã điền thông tin: " + ", ".join(notes)


def _click_add_detail(page, patterns: tuple[str, ...]) -> bool:
    """Bấm Add/Thêm cạnh nhãn phone/email/… nếu ô chưa mở."""
    joined = "|".join(patterns)
    pattern = re.compile(rf"(?:add|thêm|edit|chỉnh sửa).*(?:{joined})|(?:{joined})", re.I)
    try:
        buttons = page.get_by_role("button", name=pattern)
        count = min(buttons.count(), 10)
    except Exception:  # noqa: BLE001
        return False
    for index in range(count):
        item = buttons.nth(index)
        try:
            if item.is_visible():
                item.click(timeout=4_000)
                _step_pause(page)
                return True
        except Exception:  # noqa: BLE001
            continue
    return False


def _click_edit_details(page) -> bool:
    pattern = re.compile(r"edit|chỉnh sửa|chinh sua|add|thêm|update|cập nhật", re.I)
    try:
        buttons = page.get_by_role("button", name=pattern)
        count = min(buttons.count(), 12)
    except Exception:  # noqa: BLE001
        return False
    for index in range(count):
        item = buttons.nth(index)
        try:
            if item.is_visible():
                item.click(timeout=4_000)
                return True
        except Exception:  # noqa: BLE001
            continue
    return False


def _click_save_details(page) -> bool:
    pattern = re.compile(r"save|lưu|done|xong|publish|đăng", re.I)
    try:
        buttons = page.get_by_role("button", name=pattern)
        count = min(buttons.count(), 10)
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
        _step_pause(page)
        return True
    except Exception:  # noqa: BLE001
        return False


def _fill_category_if_present(page, preferred: str = "") -> bool:
    """
    Chọn đúng MỘT hạng mục rồi dừng.

    Đã có chip (kể cả khi ô còn chữ «Product/servi») thì xóa chữ đang gõ,
    không gõ hạng mục thứ hai, rồi chuyển sang Bio để bấm Next.
    """
    try:
        target = _category_box(page)
        if target is None:
            return True
        english = _form_is_english(page)
        from .page_details import categories_are_same, category_label_for_form

        wanted = category_label_for_form(
            preferred or ("Product/service" if english else "Sản phẩm/Dịch vụ"),
            english=english,
        )
        if _category_chip_matches(page, wanted):
            _release_category_for_next(page)
            return True
        if _committed_category_labels(page):
            logger.info("Gỡ hạng mục đang chọn vì không phải «{}».", wanted)
            _wipe_all_category_chips(page)

        target = _category_box(page) or target
        selected = _select_category_option(page, target, wanted)
        if _category_chip_matches(page, wanted) or (selected and _category_chip_matches(page, wanted)):
            _release_category_for_next(page)
            return True

        fallback = "Product/service" if english else "Sản phẩm/Dịch vụ"
        if not categories_are_same(fallback, wanted):
            _release_category_for_next(page)
            _wipe_all_category_chips(page)
            target = _category_box(page) or target
            _select_category_option(page, target, fallback)
            if _category_chip_matches(page, fallback):
                _release_category_for_next(page)
                return True

        _release_category_for_next(page)
        return _category_chip_matches(page, wanted)
    except Exception as exc:  # noqa: BLE001
        logger.debug("Không điền được danh mục Page: {}", exc)
        _release_category_for_next(page)
        return False


def _category_chip_matches(page, wanted: str) -> bool:
    """Đúng một chip và chip đó đúng hạng mục cần chọn."""
    labels = _committed_category_labels(page)
    if len(labels) != 1:
        return False
    return category_option_matches(labels[0], wanted)


def _stop_if_category_chosen(page, wanted: str = "") -> bool:
    """Đã có chip thì không gõ thêm. Trả True khi có thể sang bước Bio/Next."""
    labels = _committed_category_labels(page)
    if not labels:
        return False
    if len(labels) > 1:
        _keep_only_one_category_chip(page, wanted)
    _release_category_for_next(page)
    return bool(_committed_category_labels(page)) or _single_valid_category(page)


def _release_category_for_next(page) -> None:
    """Đóng dropdown và xóa chữ đang gõ. Không Control+A vì tổ hợp đó gỡ luôn chip."""
    _close_category_list(page)
    _clear_category_query_only(page)
    _close_category_list(page)


def _describe_step_visible(page) -> bool:
    """Bước Details «How do you want to describe the Page?»."""
    return _text_visible(
        page,
        re.compile(r"How do you want to describe the Page|Bạn muốn mô tả Trang|Mô tả Trang", re.I),
    )


def _category_already_chosen(page) -> bool:
    """Đã có chip, kể cả khi ô tìm còn gõ lại đúng tên chip."""
    if _valid_category_chip_labels(page):
        return True
    if _category_list_open(page):
        _close_category_list(page)
        if _valid_category_chip_labels(page):
            return True
    box = _category_box(page)
    if box is None or _category_list_open(page):
        return False
    try:
        blob = " ".join((box.inner_text(timeout=1_000) or "").split())
    except Exception:  # noqa: BLE001
        return False
    value = " ".join(_control_value(box).split())
    from .page_details import category_pool_labels

    return any(category_label_is_selected(blob, value, label) for label in category_pool_labels())


def _category_search_input(box):
    """Ô gõ bên trong combobox. Chip nằm cạnh, không nằm trong input này."""
    if box is None:
        return None
    try:
        inner = box.locator("input")
        if inner.count():
            return inner.first
    except Exception:  # noqa: BLE001
        pass
    return box


def _clear_category_query_only(page) -> None:
    """Xóa chữ tìm bằng Backspace. Dừng khi ô trống để không gỡ chip."""
    box = _category_box(page)
    field = _category_search_input(box)
    if field is None:
        return
    try:
        value = field.input_value(timeout=1_000) or ""
    except Exception:  # noqa: BLE001
        value = ""
    if not str(value).strip():
        return
    try:
        field.click(timeout=2_000)
        field.press("End")
    except Exception:  # noqa: BLE001
        return
    for _ in range(min(len(value) + 2, 80)):
        try:
            current = field.input_value(timeout=500) or ""
        except Exception:  # noqa: BLE001
            break
        if not str(current).strip():
            break
        try:
            field.press("Backspace")
        except Exception:  # noqa: BLE001
            break


def _committed_category_labels(page) -> list[str]:
    """Chip đã chốt. Đóng list trước khi đọc chữ trong ô, để không nhầm option đang mở."""
    labels = _valid_category_chip_labels(page)
    if labels:
        return labels
    if _category_list_open(page):
        _close_category_list(page)
        labels = _valid_category_chip_labels(page)
        if labels:
            return labels
    return _labels_from_category_box_text(page)


def _labels_from_category_box_text(page) -> list[str]:
    """Đọc chữ chip trong ô Category khi nút Remove không lộ tên hạng mục."""
    box = _category_box(page)
    if box is None or _category_list_open(page):
        return []
    try:
        blob = " ".join((box.inner_text(timeout=1_000) or "").split())
    except Exception:  # noqa: BLE001
        return []
    if not blob:
        return []
    value = " ".join(_control_value(box).split())
    from .page_details import category_pool_labels

    found: list[str] = []
    seen: set[str] = set()
    for label in category_pool_labels():
        if _is_blocked_category_label(label):
            continue
        if not category_label_is_selected(blob, value, label):
            continue
        folded = " ".join(label.casefold().replace("／", "/").split())
        if folded not in seen:
            seen.add(folded)
            found.append(label)
    return found


def _finalize_category_field(page, target) -> None:
    """Đóng dropdown + xóa chữ đang gõ trong ô Category."""
    _close_category_list(page)
    if target is not None:
        _clear_category_search(page, target)


def _single_valid_category(page) -> bool:
    """True khi còn đúng một chip hạng mục hợp lệ (không Local service / Shopping)."""
    if _has_blocked_category_chip(page):
        return False
    return len(_committed_category_labels(page)) == 1


def _valid_category_chip_labels(page) -> list[str]:
    labels: list[str] = []
    seen: set[str] = set()
    for raw in _list_category_chip_labels(page):
        if _is_blocked_category_label(raw):
            continue
        if not (_is_category_chip_label(raw) or category_option_matches(raw)):
            continue
        key = " ".join(raw.casefold().replace("／", "/").split())
        if key and key not in seen:
            seen.add(key)
            labels.append(raw)
    return labels


def _category_selected_ok(page, wanted: str = "") -> bool:
    """Đủ điều kiện Next: đúng một chip hợp lệ. ``wanted`` ưu tiên khớp, không bắt buộc."""
    if not _single_valid_category(page):
        return False
    if not wanted:
        return True
    labels = _valid_category_chip_labels(page)
    return bool(labels) and (
        category_option_matches(labels[0], wanted) or category_option_matches(wanted, labels[0])
    )


def _wipe_all_category_chips(page) -> None:
    """Gỡ hết chip hạng mục trước khi chọn lại."""
    for _ in range(8):
        if not _list_category_chip_labels(page):
            return
        if _remove_category_chips(page, keep="") <= 0:
            break


def _keep_only_one_category_chip(page, preferred: str = "") -> None:
    """Nếu có >1 chip, giữ một chip (ưu tiên preferred), gỡ phần còn lại."""
    labels = _valid_category_chip_labels(page)
    if len(labels) <= 1 and not _has_blocked_category_chip(page):
        _remove_blocked_category_chips(page)
        return
    keep = ""
    if preferred:
        for label in labels:
            if category_option_matches(label, preferred):
                keep = label
                break
    if not keep and labels:
        keep = labels[0]
    _remove_blocked_category_chips(page)
    _remove_category_chips(page, keep=keep)
    for _ in range(5):
        left = _valid_category_chip_labels(page)
        if len(left) <= 1:
            break
        _remove_category_chips(page, keep=left[0])


def _clear_category_search(page, target) -> None:
    """Xóa chữ đang gõ trong ô Category (vd. Brand chưa thành chip)."""
    if target is None:
        return
    current = " ".join(_control_value(target).split())
    if not current:
        return
    _clear_control(target)


def _focus_bio_field(page) -> bool:
    """Chuyển focus từ Category sang ô Bio sau khi đã chọn hạng mục."""
    target = _bio_textbox(page)
    if target is None:
        return False
    try:
        target.click(timeout=3_000)
        _step_pause(page)
        return True
    except Exception:  # noqa: BLE001
        return False


def _select_category_option(page, target, query: str) -> bool:
    """Gõ để lọc, rồi chỉ bấm option đúng tên. Không bấm Website khi cần Entertainment website."""
    wanted = " ".join((query or "").split())
    if not wanted or target is None:
        return False
    if _category_chip_matches(page, wanted):
        logger.info("Đã có đúng hạng mục «{}».", wanted)
        _clear_category_query_only(page)
        return True
    for typed in _category_search_queries(wanted):
        if not _type_category_query(page, target, typed):
            continue
        if not _click_exact_category_option(page, wanted):
            continue
        for _wait in range(4):
            if _category_chip_matches(page, wanted):
                return True
            try:
                page.wait_for_timeout(400)
            except Exception:  # noqa: BLE001
                break
    logger.debug("Không thấy option đúng «{}» — không bấm dòng khác.", wanted)
    _clear_category_query_only(page)
    _close_category_list(page)
    return _category_chip_matches(page, wanted)


def _category_search_queries(wanted: str) -> list[str]:
    """Gõ đủ tên trước. Dấu / làm Facebook lọc lệch thì gõ phần trước dấu /."""
    text = " ".join((wanted or "").split())
    queries: list[str] = []
    for item in (text, text.split("/")[0].strip(), text.split()[0] if text else ""):
        if item and item not in queries and len(item) >= 3:
            queries.append(item)
    return queries


def _type_category_query(page, target, typed: str) -> bool:
    try:
        target.click(timeout=5_000)
        _step_pause(page)
        field = _category_search_input(target) or target
        current = " ".join(_control_value(field).split())
        if current:
            _clear_category_query_only(page)
            _step_pause(page)
        field.type(typed, delay=random.randint(60, 120))
        _step_pause(page)
        return True
    except Exception as exc:  # noqa: BLE001
        logger.debug("Không gõ được hạng mục «{}»: {}", typed, exc)
        return False


def _click_exact_category_option(page, wanted: str) -> bool:
    option = _wait_category_option(page, wanted)
    if option is None:
        return False
    label = _control_label(option)
    if not category_option_matches(label, wanted) or _is_blocked_category_label(label):
        return False
    try:
        option.click(timeout=5_000)
        _step_pause(page)
        _clear_category_query_only(page)
        _close_category_list(page)
        return True
    except Exception as exc:  # noqa: BLE001
        logger.debug("Không bấm được option hạng mục «{}»: {}", wanted, exc)
        return False


def _is_blocked_category_label(label: str) -> bool:
    text = " ".join(_first_line(label).casefold().replace("／", "/").split())
    blocked = (
        "legal",
        "local service",
        "shopping",
        "restaurant",
        "real estate",
        "sports & recreation",
        "sports and recreation",
    )
    return any(token in text for token in blocked)


def _remove_blocked_category_chips(page) -> int:
    return _remove_category_chips(page, keep="", only_blocked=True)


def _remove_category_chips(page, keep: str = "", *, only_blocked: bool = False) -> int:
    """Gỡ chip hạng mục. ``keep`` = giữ chip khớp nhãn này; rỗng = gỡ hết (hoặc chỉ blocked)."""
    removed = 0
    keep_n = " ".join((keep or "").casefold().replace("／", "/").split())
    for _ in range(8):
        chip = None
        for item in _iter_category_remove_buttons(page):
            label = _control_label(item)
            if only_blocked and not _is_blocked_category_label(label):
                continue
            if keep_n and category_option_matches(label, keep):
                continue
            if keep_n and keep_n in label.casefold().replace("／", "/"):
                continue
            chip = item
            break
        if chip is None:
            break
        try:
            chip.click(timeout=3_000)
            removed += 1
            _step_pause(page)
        except Exception:  # noqa: BLE001
            break
    return removed


def _iter_category_remove_buttons(page):
    pattern = re.compile(
        r"remove\s+.+|xóa\s+.+|dismiss\s+.+"
        r"|local service|product\s*/\s*service|sản phẩm|shopping|community|brand|"
        r"entertainment|trang web|digital creator|blog|legal|restaurant",
        re.I,
    )
    for root in _iter_roots(page):
        for role in ("button", "link"):
            try:
                loc = root.get_by_role(role, name=pattern)
                count = min(loc.count(), 16)
            except Exception:  # noqa: BLE001
                continue
            for index in range(count):
                item = loc.nth(index)
                try:
                    if not item.is_visible():
                        continue
                    label = _control_label(item)
                except Exception:  # noqa: BLE001
                    continue
                if _blocked_click(label) and "remove" not in label.casefold() and "xóa" not in label.casefold():
                    continue
                # Nút Remove … hoặc chip tên hạng mục có ×.
                if re.search(r"remove|xóa|dismiss", label, re.I) or _is_category_chip_label(label):
                    yield item


def _is_category_chip_label(label: str) -> bool:
    text = " ".join(_first_line(label).casefold().replace("／", "/").split())
    if not text or len(text) > 60:
        return False
    if _is_blocked_category_label(text):
        return True
    from .page_details import category_pool_labels

    return any(
        text == " ".join(item.casefold().replace("／", "/").split())
        or item.casefold().replace("／", "/") in text
        for item in category_pool_labels()
    )


def _has_blocked_category_chip(page) -> bool:
    return any(_is_blocked_category_label(_control_label(item)) for item in _iter_category_remove_buttons(page))


def _list_category_chip_labels(page) -> list[str]:
    labels: list[str] = []
    for item in _iter_category_remove_buttons(page):
        label = _first_line(_control_label(item))
        # Chuẩn hóa «Remove Product/service» → Product/service
        cleaned = re.sub(r"^(?:remove|xóa|dismiss)\s+", "", label, flags=re.I).strip()
        if cleaned and cleaned not in labels:
            labels.append(cleaned)
    return labels


def _wait_category_option(page, wanted: str, *, timeout_ms: int = 4_500):
    """Chờ listbox hiện option khớp hạng mục."""
    deadline = time.time() + max(timeout_ms, 500) / 1000.0
    while time.time() < deadline:
        option = _matching_category_option(page, wanted)
        if option is not None:
            return option
        try:
            page.wait_for_timeout(250)
        except Exception:  # noqa: BLE001
            break
    return _matching_category_option(page, wanted)


def _matching_category_option(page, wanted: str = ""):
    found = None
    for root in _iter_roots(page):
        for role in ("option", "menuitem", "listitem"):
            try:
                loc = root.get_by_role(role)
                count = min(loc.count(), 20)
            except Exception:  # noqa: BLE001
                continue
            for index in range(count):
                item = loc.nth(index)
                try:
                    if not item.is_visible():
                        continue
                    label = _control_label(item)
                except Exception:  # noqa: BLE001
                    continue
                if category_option_matches(label, wanted or None):
                    return item
        try:
            if wanted:
                pattern = re.compile(re.escape(wanted), re.I)
            else:
                pattern = re.compile(
                    r"Product\s*/\s*services?|Sản phẩm\s*/\s*Dịch vụ|Community|Cộng đồng|Brand|Thương hiệu",
                    re.I,
                )
            loc = root.get_by_text(pattern)
            count = min(loc.count(), 10)
        except Exception:  # noqa: BLE001
            continue
        for index in range(count):
            item = loc.nth(index)
            try:
                if not item.is_visible():
                    continue
                label = _control_label(item)
                # Tránh bấm lại đúng chữ đang nằm trong ô combobox.
                if _looks_like_category_search_field(item):
                    continue
                if category_option_matches(label, wanted or None):
                    found = item
                    break
            except Exception:  # noqa: BLE001
                continue
        if found is not None:
            return found
    return None


def _looks_like_category_search_field(item) -> bool:
    try:
        role = (item.get_attribute("role") or "").casefold()
    except Exception:  # noqa: BLE001
        role = ""
    return role in {"combobox", "textbox", "searchbox"}


def _category_list_open(page) -> bool:
    box = _category_box(page)
    if box is not None:
        try:
            if (box.get_attribute("aria-expanded") or "").casefold() == "true":
                return True
        except Exception:  # noqa: BLE001
            pass
    for root in _iter_roots(page):
        try:
            listed = root.get_by_role("listbox")
            if listed.count() and listed.first.is_visible():
                return True
        except Exception:  # noqa: BLE001
            continue
    return False


def _category_chip_visible(page, preferred: str = "") -> bool:
    """
    True chỉ khi hạng mục đã được chọn thật (chip), không phải chữ đang gõ trong ô.

    Ví dụ sai: ô còn «Interest» + hint «Enter a category…» → chưa chọn.
    """
    if _category_list_open(page):
        return False
    if _find_selected_category_chip(page, preferred) is not None:
        return True

    box = _category_box(page)
    if box is None:
        return False
    value = " ".join(_control_value(box).split())
    if not value:
        return False
    # Chữ còn trong combobox trùng query đang tìm = chưa chọn option.
    if _category_value_is_pending_query(value, preferred):
        return False
    # Một số UI để nguyên tên hạng mục trong ô sau khi chọn — cần có dấu hiệu chip/remove.
    if _category_has_remove_control(page, value):
        blob = f"{value} {_control_label(box)}"
        if preferred and category_option_matches(blob, preferred):
            return True
        from .page_details import category_pool_labels

        return any(category_option_matches(blob, label) for label in category_pool_labels())
    return False


def _category_value_is_pending_query(value: str, preferred: str = "") -> bool:
    """Ô còn đúng chữ vừa gõ để tìm — chưa phải chip đã chốt."""
    typed = " ".join((value or "").split()).casefold().replace("／", "/")
    if not typed:
        return False
    want = " ".join((preferred or "").split()).casefold().replace("／", "/")
    if want and (typed == want or (len(typed) >= 4 and want.startswith(typed))):
        return True
    from .page_details import category_fallbacks, category_pool_labels

    known = {
        " ".join(item.casefold().replace("／", "/").split())
        for item in (*category_pool_labels(), *category_fallbacks("en"), *category_fallbacks("vi"))
    }
    # Một từ đơn giản trùng pool (Interest, Brand, Blog…) khi chưa có chip remove → coi là đang gõ.
    if typed in known:
        return True
    # «Product/servi» là tiền tố của «Product/service» — vẫn đang gõ, chưa phải chip.
    if any(len(typed) >= 4 and label.startswith(typed) and label != typed for label in known):
        return True
    if "/" not in typed and " " not in typed and len(typed) <= 24:
        return True
    return False


def should_type_another_category(chip_labels: list[str], input_value: str = "") -> bool:
    """
    False khi đã có ít nhất một chip hạng mục.

    Chữ còn trong ô (vd. «Entertainment website» cạnh chip cùng tên) là query thừa,
    phải xóa rồi bấm Next — không gõ thêm.
    """
    del input_value
    return not any(str(label or "").strip() for label in chip_labels)


def category_label_is_selected(blob: str, input_value: str, label: str) -> bool:
    """
    True khi hạng mục đã thành chip, kể cả lúc ô tìm vẫn còn đúng chữ đó.

    «Entertainment website» + chip «Entertainment website» → đã chọn.
    Chỉ một dòng chữ trong ô, chưa có chip → chưa chọn.
    """
    folded_blob = " ".join((blob or "").split()).casefold().replace("／", "/")
    folded_value = " ".join((input_value or "").split()).casefold().replace("／", "/")
    folded = " ".join((label or "").split()).casefold().replace("／", "/")
    if not folded or folded not in folded_blob:
        return False
    occurrences = folded_blob.count(folded)
    if occurrences >= 2:
        return True
    if not folded_value:
        return True
    if folded_value == folded or folded.startswith(folded_value):
        return False
    return True


def _find_selected_category_chip(page, preferred: str = ""):
    """Tìm chip hạng mục đã chọn (thường kèm nút Remove / ×)."""
    patterns = (
        re.compile(r"remove\s+.+|xóa\s+.+|dismiss\s+.+", re.I),
        re.compile(r"product\s*/\s*service|sản phẩm\s*/\s*dịch vụ|community|cộng đồng|brand|thương hiệu", re.I),
    )
    for root in _iter_roots(page):
        for role in ("button", "link"):
            for pattern in patterns:
                try:
                    loc = root.get_by_role(role, name=pattern)
                    count = min(loc.count(), 12)
                except Exception:  # noqa: BLE001
                    continue
                for index in range(count):
                    item = loc.nth(index)
                    try:
                        if not item.is_visible():
                            continue
                        label = _control_label(item)
                    except Exception:  # noqa: BLE001
                        continue
                    if preferred and not category_option_matches(label, preferred):
                        # Remove «Interest» vẫn tính là chip của Interest.
                        if "remove" not in label.casefold() and "xóa" not in label.casefold():
                            continue
                        if preferred.casefold() not in label.casefold():
                            continue
                    if _blocked_click(label):
                        continue
                    if re.search(r"remove|xóa|dismiss|product\s*/\s*service|sản phẩm|community|brand", label, re.I):
                        return item
    return None


def _category_has_remove_control(page, category_text: str) -> bool:
    token = " ".join((category_text or "").split())
    if not token:
        return False
    pattern = re.compile(rf"(?:remove|xóa|dismiss).*{re.escape(token)}|{re.escape(token)}.*(?:remove|xóa)", re.I)
    for root in _iter_roots(page):
        try:
            loc = root.get_by_role("button", name=pattern)
            if loc.count() and loc.first.is_visible():
                return True
        except Exception:  # noqa: BLE001
            continue
    return False


def _control_label(item) -> str:
    parts: list[str] = []
    try:
        parts.append(item.inner_text(timeout=600) or "")
    except Exception:  # noqa: BLE001
        pass
    try:
        parts.append(item.get_attribute("aria-label") or "")
    except Exception:  # noqa: BLE001
        pass
    return " ".join(part for part in parts if part)


def _wait_for_created_page(page, page_name: str) -> tuple[str, str]:
    """Chờ wizard tạo xong. Không đóng form khi chưa có id."""
    for _wait in range(12):
        if _creation_confirm_open(page):
            return "", ""
        found = _find_named_page(page, page_name)
        if found[0]:
            return found
        current = _page_id_from_current(page)
        if current[0]:
            return current
        try:
            page.wait_for_timeout(1000)
        except Exception:  # noqa: BLE001
            break
    return _page_id_from_current(page)


def finish_setting_visible(page) -> bool:
    """True khi Facebook đang mở bước Finish setting / hoàn tất cài đặt Page."""
    patterns = (
        re.compile(
            r"finish setting|finish setting up|hoàn tất cài đặt|hoan tat cai dat|"
            r"set up your page|thiết lập (?:trang|page)|complete your page|"
            r"continue setting|get started on your page",
            re.I,
        ),
        re.compile(
            r"add (?:a )?profile picture|thêm ảnh đại diện|add a cover|"
            r"contact (?:info|information)|thông tin liên hệ|invite friends|mời bạn bè",
            re.I,
        ),
    )
    return any(_text_visible(page, pattern) for pattern in patterns)


def is_finish_setting_done_label(label: str) -> bool:
    """Nút Done / Hoàn tất kết thúc wizard Finish setting."""
    text = _first_line(label).casefold().strip(" .")
    if not text or is_skip_setup_label(text):
        return False
    return text in {
        "done",
        "xong",
        "finish",
        "hoàn tất",
        "hoan tat",
        "hoàn thành",
        "hoan thanh",
        "got it",
        "ok",
    }


def is_invite_step_label(label: str) -> bool:
    text = _first_line(label).casefold()
    return "invite" in text or "mời bạn" in text or "moi ban" in text


def _page_id_from_current(page) -> tuple[str, str]:
    """Đọc page id từ URL hiện tại (wizard sau Create thường đã có id)."""
    if _creation_confirm_open(page):
        return "", ""
    try:
        url = page.url or ""
    except Exception:  # noqa: BLE001
        url = ""
    page_id = created_page_id_from_url(url)
    if not page_id:
        return "", ""
    return page_id, f"https://www.facebook.com/{page_id}"


def _complete_finish_setting_wizard(page, request: PageCreationRequest) -> dict[str, object]:
    """
    Đi hết wizard Finish setting: thông tin liên hệ → ảnh → (skip mời bạn) → Done.

    Không page.goto Away. Không bấm Skip khi còn ảnh/thông tin cần điền ở bước hiện tại.
    """
    _refresh_request_details(request)
    notes: list[str] = []
    details_filled = 0
    avatar_ok = False
    if _creation_confirm_open(page):
        notes.append("finish_setting:confirm_still_open")
        return {"avatar": False, "details_filled": 0, "notes": notes}

    # Chờ wizard hiện sau Create (FB đôi khi chậm 1–2s).
    for _wait in range(16):
        if finish_setting_visible(page) or _avatar_upload_controls(page) or _page_has_image_file_input(page):
            break
        try:
            page.wait_for_timeout(400)
        except Exception:  # noqa: BLE001
            break

    if not finish_setting_visible(page) and not _avatar_upload_controls(page):
        notes.append("finish_setting:absent")
        return {"avatar": False, "details_filled": 0, "notes": notes}

    notes.append("finish_setting:open")
    for step in range(14):
        if request.should_stop():
            notes.append("finish_setting:cancelled")
            break
        blocked = _surface_code(page)
        if blocked:
            notes.append(f"finish_setting:{blocked}")
            _shot(page, f"finish_{blocked.lower()}")
            break

        step_filled = _fill_finish_setting_contacts(page, request)
        if step_filled:
            details_filled += step_filled
            notes.append(f"finish_contacts:{step_filled}")

        avatar_path = str(request.avatar_path or "").strip()
        if avatar_path and Path(avatar_path).is_file() and not avatar_ok:
            if _attach_avatar_on_surface(page, avatar_path) or _handle_post_create_avatar(page, avatar_path):
                avatar_ok = True
                notes.append("finish_avatar:ok")
                request.on_state("UPLOADING_AVATAR")
            elif _avatar_upload_controls(page) or _page_has_image_file_input(page):
                # Đang ở bước ảnh nhưng chưa gắn được — chụp và thử lại vòng sau.
                _shot(page, "finish_avatar_try")

        if _click_finish_setting_done(page):
            notes.append("finish_setting:done")
            _step_pause(page)
            break

        # Bước mời bạn bè: được Skip. Bước ảnh/thông tin: ưu tiên Next.
        if _invite_friends_step_visible(page):
            if _click_skip_setup(page) or _click_wizard_next(page):
                notes.append("finish_invite:skip")
                _step_pause(page)
                continue

        need_stay = bool(
            (
                avatar_path
                and Path(avatar_path).is_file()
                and not avatar_ok
                and (_avatar_upload_controls(page) or _page_has_image_file_input(page))
            )
            or _finish_setting_has_empty_contact(page, request)
        )
        if need_stay:
            # Còn việc trên bước này — thử Next nếu FB cho phép, không Skip.
            if _click_wizard_next(page):
                _step_pause(page)
                continue
            _step_pause(page)
            continue

        if _click_wizard_next(page):
            _step_pause(page)
            continue
        if _click_skip_setup(page):
            notes.append("finish_setting:skip_step")
            _step_pause(page)
            continue
        # Không còn nút điều hướng — thoát vòng.
        notes.append("finish_setting:stuck")
        _shot(page, "finish_setting_stuck")
        break

    avatar_path = str(request.avatar_path or "").strip()
    if avatar_path and Path(avatar_path).is_file() and not avatar_ok:
        notes.append("finish_avatar:miss")
    return {"avatar": avatar_ok, "details_filled": details_filled, "notes": notes}


def _fill_finish_setting_contacts(page, request: PageCreationRequest) -> int:
    """Điền các ô liên hệ đang hiện trên bước Finish setting."""
    mapping = (
        ("phone", ("phone", "số điện thoại", "dien thoai", "mobile")),
        ("email", ("email", "e-mail", "thư điện tử", "thu dien tu")),
        ("website", ("website", "web", "trang web", "website url", "link")),
        ("address", ("address", "địa chỉ", "dia chi", "location", "city", "vị trí")),
        ("bio", ("bio", "tiểu sử", "mô tả", "about", "giới thiệu", "description")),
    )
    filled = 0
    for key, patterns in mapping:
        value = str(getattr(request, key, "") or "").strip()
        if not value:
            continue
        if _fill_labeled_textbox(page, patterns, value):
            filled += 1
    return filled


def _finish_setting_has_empty_contact(page, request: PageCreationRequest) -> bool:
    """True nếu bước hiện tại còn ô liên hệ trống mà request có dữ liệu."""
    checks = (
        (("phone", "số điện thoại", "mobile"), request.phone),
        (("email", "e-mail", "thư điện tử"), request.email),
        (("website", "trang web", "web"), request.website),
        (("address", "địa chỉ", "location"), request.address),
        (("bio", "tiểu sử", "mô tả", "about"), request.bio),
    )
    for patterns, value in checks:
        if not str(value or "").strip():
            continue
        pattern = re.compile("|".join(patterns), re.I)
        for root in _iter_roots(page):
            try:
                box = root.get_by_role("textbox", name=pattern)
                count = min(box.count(), 4)
            except Exception:  # noqa: BLE001
                continue
            for index in range(count):
                target = box.nth(index)
                try:
                    if target.is_visible() and not " ".join(_control_value(target).split()):
                        return True
                except Exception:  # noqa: BLE001
                    continue
    return False


def _invite_friends_step_visible(page) -> bool:
    return _text_visible(
        page,
        re.compile(r"invite friends|mời bạn bè|moi ban be|build an audience|grow your audience", re.I),
    )


def _click_finish_setting_done(page) -> bool:
    for root in _iter_roots(page):
        try:
            loc = root.get_by_role("button")
            count = min(loc.count(), 12)
        except Exception:  # noqa: BLE001
            continue
        for index in range(count):
            item = loc.nth(index)
            try:
                if not item.is_visible() or not item.is_enabled():
                    continue
                label = _control_label(item)
            except Exception:  # noqa: BLE001
                continue
            if is_finish_setting_done_label(label):
                try:
                    item.click(timeout=5_000)
                    return True
                except Exception:  # noqa: BLE001
                    continue
    return False


def _click_skip_setup(page) -> bool:
    """Bấm Skip/Bỏ qua trên bước không bắt buộc (mời bạn, thông báo)."""
    for root in _iter_roots(page):
        try:
            loc = root.get_by_role("button")
            count = min(loc.count(), 12)
        except Exception:  # noqa: BLE001
            continue
        for index in range(count):
            item = loc.nth(index)
            try:
                if not item.is_visible() or not item.is_enabled():
                    continue
                label = _control_label(item)
            except Exception:  # noqa: BLE001
                continue
            if is_skip_setup_label(label):
                try:
                    item.click(timeout=4_000)
                    return True
                except Exception:  # noqa: BLE001
                    continue
    return False


def _checkbox_checked(item) -> bool:
    try:
        if item.is_checked():
            return True
    except Exception:  # noqa: BLE001
        pass
    try:
        return (item.get_attribute("aria-checked") or "").casefold() == "true"
    except Exception:  # noqa: BLE001
        return False


def _terms_checkbox(page):
    """Ô đồng ý trên bước Xác nhận. Không bấm link Điều khoản."""
    lone = None
    lone_count = 0
    name_pattern = re.compile(r"đồng ý|điều khoản|i agree|commercial terms", re.I)
    for root in _iter_roots(page):
        named = None
        try:
            named = root.get_by_role("checkbox", name=name_pattern)
            count = min(named.count(), 4)
        except Exception:  # noqa: BLE001
            count = 0
        if named is not None:
            for index in range(count):
                item = named.nth(index)
                try:
                    if item.is_visible():
                        return item
                except Exception:  # noqa: BLE001
                    continue
        try:
            loc = root.get_by_role("checkbox")
            total = min(loc.count(), 6)
        except Exception:  # noqa: BLE001
            continue
        visible = []
        for index in range(total):
            item = loc.nth(index)
            try:
                if item.is_visible():
                    visible.append(item)
            except Exception:  # noqa: BLE001
                continue
        for item in visible:
            if is_terms_checkbox_label(_control_label(item)):
                return item
        if len(visible) == 1:
            lone = visible[0]
            lone_count += 1
    if lone is not None and lone_count == 1 and _terms_copy_visible(page):
        return lone
    return None


def _terms_copy_visible(page) -> bool:
    pattern = re.compile(r"tôi đồng ý|i agree", re.I)
    for root in _iter_roots(page):
        try:
            copy = root.get_by_text(pattern)
            if copy.count() > 0 and copy.first.is_visible():
                return True
        except Exception:  # noqa: BLE001
            continue
    return False


def _click_checkbox(box) -> None:
    try:
        box.scroll_into_view_if_needed(timeout=2_000)
    except Exception:  # noqa: BLE001
        pass
    try:
        box.click(timeout=5_000)
        return
    except Exception as exc:  # noqa: BLE001
        logger.debug("Không tick được ô đồng ý: {}", exc)
    try:
        box.click(timeout=4_000, force=True)
    except Exception as forced:  # noqa: BLE001
        logger.debug("Không tick được ô đồng ý khi ép: {}", forced)


def _click_terms_sentence(page) -> None:
    """Bấm phần chữ «Thay mặt…», không bấm link Điều khoản hay Chính sách."""
    pattern = re.compile(r"Thay mặt trang quản lý|On behalf of the business", re.I)
    for root in _iter_roots(page):
        try:
            sentence = root.get_by_text(pattern).first
            if sentence.is_visible():
                sentence.click(timeout=4_000)
                return
        except Exception:  # noqa: BLE001
            continue


_AGREE_STATE_JS = """
() => {
  const norm = (s) => (s || '').replace(/\\s+/g, ' ').trim().toLowerCase();
  const checked = (box) => box.checked === true || (box.getAttribute('aria-checked') || '') === 'true';
  const roots = [...document.querySelectorAll('[role="dialog"], [aria-modal="true"]')];
  const scopes = roots.length ? roots : [document.body];
  for (const root of scopes) {
    const blob = norm(root.innerText || root.textContent);
    if (!/i agree|tôi đồng ý|are you sure you want to continue|the page will be created|điều khoản thương mại/.test(blob)) {
      continue;
    }
    const boxes = [...root.querySelectorAll('input[type="checkbox"], [role="checkbox"]')];
    if (boxes.some(checked)) return { state: 'checked' };
    return { state: 'open' };
  }
  return { state: 'absent' };
}
"""

_AGREE_POINT_JS = """
() => {
  const norm = (s) => (s || '').replace(/\\s+/g, ' ').trim().toLowerCase();
  const checked = (box) => box.checked === true || (box.getAttribute('aria-checked') || '') === 'true';
  const roots = [...document.querySelectorAll('[role="dialog"], [aria-modal="true"]')];
  const scopes = roots.length ? roots : [document.body];
  for (const root of scopes) {
    const blob = norm(root.innerText || root.textContent);
    if (!/i agree|tôi đồng ý|are you sure you want to continue|the page will be created|điều khoản thương mại/.test(blob)) {
      continue;
    }
    const boxes = [...root.querySelectorAll('input[type="checkbox"], [role="checkbox"]')];
    for (const box of boxes) {
      if (checked(box)) return { state: 'checked' };
      const rect = box.getBoundingClientRect();
      if (rect.width >= 8 && rect.height >= 8) {
        return { state: 'point', x: rect.left + rect.width / 2, y: rect.top + rect.height / 2 };
      }
    }
    const rows = [...root.querySelectorAll('label, div, span')];
    for (const row of rows) {
      const text = norm(row.innerText || '');
      if (!text.startsWith('i agree') && !text.startsWith('tôi đồng ý')) continue;
      const rect = row.getBoundingClientRect();
      if (rect.width < 40 || rect.height < 12 || rect.height > 90) continue;
      return { state: 'point', x: rect.left + 14, y: rect.top + rect.height / 2 };
    }
  }
  return { state: 'miss' };
}
"""


def _creation_confirm_open(page) -> bool:
    """Hộp «Are you sure you want to continue?» — phải tick rồi mới bấm Tạo Trang."""
    if _text_visible(page, _CREATE_CONFIRM_MARKERS):
        return True
    _frame, result = _eval_agree_script(page, _AGREE_STATE_JS)
    return isinstance(result, dict) and result.get("state") in {"open", "checked"}


def _eval_agree_script(page, script) -> tuple[object, dict]:
    """Chạy script trong trang và từng frame. Trả frame cùng kết quả."""
    frames = [page]
    try:
        frames = list(page.frames) or [page]
    except Exception:  # noqa: BLE001
        frames = [page]
    for frame in frames:
        try:
            result = frame.evaluate(script)
        except Exception:  # noqa: BLE001
            continue
        if isinstance(result, dict) and result.get("state") not in {None, "absent", "miss"}:
            return frame, result
    return None, {"state": "absent"}


def _mouse_click_in_frame(page, frame, x: float, y: float) -> None:
    """Click thật tại tọa độ. Cộng lệch iframe nếu ô điều khoản không nằm ở trang chính."""
    ox, oy = 0.0, 0.0
    try:
        main = page.main_frame
    except Exception:  # noqa: BLE001
        main = None
    if frame is not None and main is not None and frame != main and frame != page:
        try:
            box = frame.frame_element().bounding_box()
            if box:
                ox, oy = float(box["x"]), float(box["y"])
        except Exception:  # noqa: BLE001
            ox, oy = 0.0, 0.0
    page.mouse.click(ox + float(x), oy + float(y))


def _agreement_is_checked(page) -> bool:
    box = _terms_checkbox(page)
    if box is not None and _checkbox_checked(box):
        return True
    _frame, result = _eval_agree_script(page, _AGREE_STATE_JS)
    return isinstance(result, dict) and result.get("state") == "checked"


def _tick_agreement_box(page) -> bool:
    """Tick ô «I agree…» bằng click chuột tại ô vuông, không bấm link điều khoản."""
    if _agreement_is_checked(page):
        return True
    frame, result = _eval_agree_script(page, _AGREE_POINT_JS)
    if isinstance(result, dict) and result.get("state") == "checked":
        return True
    if isinstance(result, dict) and result.get("state") == "point" and frame is not None:
        try:
            _mouse_click_in_frame(page, frame, float(result.get("x") or 0), float(result.get("y") or 0))
        except Exception as exc:  # noqa: BLE001
            logger.debug("Không bấm được ô điều khoản: {}", exc)
        _step_pause(page)
        if _agreement_is_checked(page):
            logger.info("Đã tick ô đồng ý điều khoản.")
            return True
    box = _terms_checkbox(page)
    if box is not None:
        _click_checkbox(box)
        _step_pause(page)
    if _agreement_is_checked(page):
        logger.info("Đã tick ô đồng ý điều khoản.")
        return True
    return False


def _accept_creation_terms(page) -> bool:
    """Tick ô đồng ý nếu bước Xác nhận đang hiện. Bước Chi tiết không có ô thì bỏ qua."""
    confirm = _creation_confirm_open(page)
    if not confirm and not _terms_copy_visible(page):
        return True
    if _tick_agreement_box(page):
        return True
    _click_terms_sentence(page)
    _step_pause(page)
    if _agreement_is_checked(page):
        return True
    box = _terms_checkbox(page)
    return (not confirm) and box is not None and _checkbox_checked(box)


def _wizard_scopes(page) -> list:
    """Hộp thoại tạo Page đứng trước nền Business Suite, để không bỏ sót nút Next."""
    scopes = []
    for root in _iter_roots(page):
        try:
            dialogs = root.get_by_role("dialog")
            total = min(dialogs.count(), 5)
        except Exception:  # noqa: BLE001
            total = 0
        for index in range(total):
            item = dialogs.nth(index)
            try:
                if item.is_visible():
                    scopes.append(item)
            except Exception:  # noqa: BLE001
                continue
    if scopes:
        return scopes
    return list(_iter_roots(page))


def _leave_details_with_next(page) -> bool:
    """Bấm Next trên bước tên / hạng mục / mô tả. Không bấm Tạo Trang."""
    for _try in range(4):
        if _creation_confirm_open(page) or _create_page_button_visible(page):
            return True
        if not _describe_step_visible(page) and not _category_box(page):
            return True
        _release_category_for_next(page)
        if _click_wizard_next(page, only_rank=2):
            _step_pause(page)
            continue
        _step_pause(page)
    return _creation_confirm_open(page) or _create_page_button_visible(page) or not _describe_step_visible(page)


def _tick_and_create_page(page) -> bool:
    """Tick ô đồng ý rồi bấm Tạo Trang. Chưa rời hộp xác nhận thì chưa tính là tạo xong."""
    for _try in range(8):
        on_confirm = (
            _creation_confirm_open(page) or _terms_copy_visible(page) or _create_page_button_visible(page)
        )
        if on_confirm:
            if (_creation_confirm_open(page) or _terms_copy_visible(page)) and not _accept_creation_terms(page):
                _step_pause(page)
                continue
            if _create_page_button_visible(page):
                if not _click_wizard_next(page, only_rank=3):
                    _step_pause(page)
                    continue
                _step_pause(page)
                if not _creation_confirm_open(page) and not _create_page_button_visible(page):
                    return True
                continue
            _step_pause(page)
            continue
        if _describe_step_visible(page):
            _click_wizard_next(page, only_rank=2)
            _step_pause(page)
            continue
        if _try >= 2:
            return True
        _step_pause(page)
    return not _creation_confirm_open(page) and not _create_page_button_visible(page)


def _capture_created_page(page, request: PageCreationRequest) -> tuple[str, str]:
    """Đọc UID và link sau khi hộp Tạo Trang đã đóng."""
    if _creation_confirm_open(page):
        return "", ""
    page_id, page_url = _wait_for_created_page(page, request.page_name)
    if not page_id:
        page_id, page_url = _page_id_from_current(page)
    if not page_id and (request.create_mode or "").casefold() == "profile":
        page_id, page_url = _wait_for_profile_page(page, request.page_name)
    if not page_id:
        page_id, page_url = _search_pages_list(page, request.page_name)
    if page_id and not page_url:
        page_url = f"https://www.facebook.com/{page_id}"
    return page_id, page_url


def _create_page_button_visible(page) -> bool:
    """Nút Tạo Trang / Create Page đang hiện — bước xác nhận, chưa phải lúc gắn ảnh."""
    for scope in _wizard_scopes(page):
        try:
            loc = scope.get_by_role("button", name=_WIZARD_ACTION_NAME)
            count = min(loc.count(), 8)
        except Exception:  # noqa: BLE001
            continue
        for index in range(count):
            item = loc.nth(index)
            try:
                if item.is_visible() and confirm_button_rank(_control_label(item)) >= 3:
                    return True
            except Exception:  # noqa: BLE001
                continue
    return False


def _click_wizard_next(page, *, only_rank: int = 0) -> bool:
    """Bấm Next hoặc Tạo Trang. ``only_rank=2`` chỉ Next, ``only_rank=3`` chỉ Tạo Trang."""
    if _category_list_open(page):
        _close_category_list(page)
    chosen = None
    best_rank = 0
    for scope in _wizard_scopes(page):
        try:
            loc = scope.get_by_role("button", name=_WIZARD_ACTION_NAME)
            count = min(loc.count(), 8)
        except Exception:  # noqa: BLE001
            continue
        for index in range(count):
            item = loc.nth(index)
            try:
                if not item.is_visible():
                    continue
                rank = confirm_button_rank(_control_label(item)) or 2
            except Exception:  # noqa: BLE001
                continue
            if only_rank == 2 and rank != 2:
                continue
            if only_rank == 3 and rank < 3:
                continue
            if rank >= best_rank:
                chosen = item
                best_rank = rank
    if chosen is None:
        logger.debug("Không thấy nút Next trong hộp thoại tạo Page.")
        return False
    try:
        chosen.scroll_into_view_if_needed(timeout=2_000)
    except Exception:  # noqa: BLE001
        pass
    if best_rank >= 3:
        if _creation_confirm_open(page) and not _accept_creation_terms(page):
            logger.info("Nút Tạo Trang còn khóa vì chưa tick điều khoản.")
            return False
        enabled = False
        for _wait in range(12):
            try:
                if chosen.is_enabled() and _button_allows_click(chosen):
                    enabled = True
                    break
            except Exception:  # noqa: BLE001
                break
            try:
                page.wait_for_timeout(400)
            except Exception:  # noqa: BLE001
                break
        if not enabled:
            logger.debug("Nút Tạo Trang vẫn khóa, chưa bấm.")
            return False
        return _click_control_center(page, chosen)
    if not _button_allows_click(chosen):
        logger.info("Nút Next còn khóa — hạng mục hoặc Bio chưa đủ, không bấm.")
        return False
    try:
        chosen.click(timeout=8_000)
        return True
    except Exception as exc:  # noqa: BLE001
        logger.debug("Không bấm được Next: {}", exc)
        return False


def _button_allows_click(item) -> bool:
    """Nút sáng. Nút Create Page xám (aria-disabled) thì chưa bấm."""
    try:
        allowed = item.evaluate(
            """el => el.getAttribute('aria-disabled') !== 'true' && !el.hasAttribute('disabled')"""
        )
        return bool(allowed)
    except Exception:  # noqa: BLE001
        return True


def _click_control_center(page, item) -> bool:
    """Bấm giữa nút bằng chuột thật. Không ép click khi nút còn khóa."""
    try:
        box = item.bounding_box()
    except Exception:  # noqa: BLE001
        box = None
    if box:
        try:
            page.mouse.click(float(box["x"]) + float(box["width"]) / 2, float(box["y"]) + float(box["height"]) / 2)
            return True
        except Exception as exc:  # noqa: BLE001
            logger.debug("Không bấm được giữa nút Tạo Trang: {}", exc)
    try:
        item.click(timeout=8_000)
        return True
    except Exception as exc:  # noqa: BLE001
        logger.debug("Không bấm được Tạo Trang: {}", exc)
        return False


def is_avatar_upload_label(label: str) -> bool:
    """Nút/thẻ mở chọn ảnh đại diện Page — không nhận Skip/Bỏ qua."""
    text = _first_line(label).casefold()
    if not text or is_skip_setup_label(text):
        return False
    markers = (
        "profile picture",
        "profile photo",
        "ảnh đại diện",
        "anh dai dien",
        "add photo",
        "upload photo",
        "upload picture",
        "thêm ảnh",
        "them anh",
        "chọn ảnh",
        "chon anh",
        "edit picture",
        "update picture",
        "đổi ảnh",
        "doi anh",
        "cập nhật ảnh",
        "camera",
    )
    return any(marker in text for marker in markers)


def is_skip_setup_label(label: str) -> bool:
    """Skip / Bỏ qua / Not now trên wizard sau Create — không bấm khi còn ảnh cần tải."""
    text = _first_line(label).casefold().strip(" .")
    return text in {
        "skip",
        "bỏ qua",
        "bo qua",
        "not now",
        "để sau",
        "de sau",
        "maybe later",
        "later",
        "close",
        "đóng",
        "dong",
    }


def is_save_photo_label(label: str) -> bool:
    """Lưu/Save sau khi chọn ảnh đại diện."""
    text = _first_line(label).casefold().strip(" .")
    if not text or is_skip_setup_label(text):
        return False
    return text in {
        "save",
        "lưu",
        "luu",
        "done",
        "xong",
        "apply",
        "áp dụng",
        "ap dung",
        "save photo",
        "lưu ảnh",
        "save changes",
        "lưu thay đổi",
    }


def _attach_avatar(page, avatar_path: str) -> bool:
    """Alias cũ — gắn ảnh trên mặt hiện tại."""
    return _attach_avatar_on_surface(page, avatar_path)


def _attach_avatar_on_surface(page, avatar_path: str) -> bool:
    """Gắn file ảnh vào input[type=file] hoặc qua file chooser sau khi bấm nút upload."""
    path = Path(avatar_path)
    if not path.is_file():
        return False
    if _set_image_file_inputs(page, str(path)):
        _step_pause(page)
        return True
    return _upload_via_file_chooser(page, str(path))


def _set_image_file_inputs(page, avatar_path: str) -> bool:
    try:
        files = page.locator("input[type='file']")
        count = min(files.count(), 8)
    except Exception:  # noqa: BLE001
        return False
    for index in range(count):
        item = files.nth(index)
        try:
            accept = (item.get_attribute("accept") or "").casefold()
        except Exception:  # noqa: BLE001
            accept = ""
        if accept and "image" not in accept and "*" not in accept:
            continue
        try:
            item.set_input_files(avatar_path)
            return True
        except Exception as exc:  # noqa: BLE001
            logger.debug("set_input_files thất bại (ô {}): {}", index, exc)
    return False


def _upload_via_file_chooser(page, avatar_path: str) -> bool:
    """Bấm nút Add profile picture rồi gắn file qua Playwright file chooser."""
    buttons = _avatar_upload_controls(page)
    for control in buttons:
        try:
            with page.expect_file_chooser(timeout=6_000) as chooser_info:
                control.click(timeout=5_000)
            chooser_info.value.set_files(avatar_path)
            _step_pause(page)
            return True
        except Exception as exc:  # noqa: BLE001
            logger.debug("File chooser avatar thất bại: {}", exc)
            # Có thể click đã mở menu — thử input file vừa xuất hiện.
            if _set_image_file_inputs(page, avatar_path):
                return True
    return False


def _avatar_upload_controls(page) -> list:
    found = []
    pattern = re.compile(
        r"profile picture|profile photo|ảnh đại diện|add photo|upload|thêm ảnh|chọn ảnh|camera|đổi ảnh",
        re.I,
    )
    for root in _iter_roots(page):
        for role in ("button", "link", "menuitem"):
            try:
                loc = root.get_by_role(role, name=pattern)
                count = min(loc.count(), 8)
            except Exception:  # noqa: BLE001
                continue
            for index in range(count):
                item = loc.nth(index)
                try:
                    if not item.is_visible():
                        continue
                    label = _control_label(item)
                except Exception:  # noqa: BLE001
                    continue
                if is_avatar_upload_label(label):
                    found.append(item)
    return found


def _confirm_photo_dialog(page) -> bool:
    """Bấm Save/Done sau khi chọn ảnh; không bấm Skip."""
    for _wait in range(8):
        for root in _iter_roots(page):
            try:
                loc = root.get_by_role("button")
                count = min(loc.count(), 12)
            except Exception:  # noqa: BLE001
                continue
            chosen = None
            for index in range(count):
                item = loc.nth(index)
                try:
                    if not item.is_visible() or not item.is_enabled():
                        continue
                    label = _control_label(item)
                except Exception:  # noqa: BLE001
                    continue
                if is_skip_setup_label(label):
                    continue
                if is_save_photo_label(label):
                    chosen = item
                    break
            if chosen is not None:
                try:
                    chosen.click(timeout=5_000)
                    _step_pause(page)
                    return True
                except Exception as exc:  # noqa: BLE001
                    logger.debug("Không bấm Save ảnh: {}", exc)
        try:
            page.wait_for_timeout(350)
        except Exception:  # noqa: BLE001
            break
    return False


def _handle_post_create_avatar(page, avatar_path: str) -> bool:
    """
    Wizard ngay sau Create Page: thêm ảnh đại diện rồi Save.

    Không bấm Skip/Bỏ qua khi còn đường dẫn ảnh hợp lệ.
    """
    if not Path(avatar_path).is_file():
        return False
    # Chờ UI setup hiện (ảnh / next step).
    for _wait in range(10):
        if _avatar_upload_controls(page) or _page_has_image_file_input(page):
            break
        try:
            page.wait_for_timeout(400)
        except Exception:  # noqa: BLE001
            break
    if not _attach_avatar_on_surface(page, avatar_path):
        return False
    _confirm_photo_dialog(page)
    # Một số flow cần Next sau Save.
    try:
        _click_wizard_next(page)
    except Exception:  # noqa: BLE001
        pass
    return True


def _page_has_image_file_input(page) -> bool:
    try:
        files = page.locator("input[type='file']")
        return files.count() > 0
    except Exception:  # noqa: BLE001
        return False


def _upload_avatar_on_page_profile(page, page_id: str, avatar_path: str) -> bool:
    """Mở trang Page rồi upload ảnh đại diện nếu wizard sau Create đã qua."""
    pid = (page_id or "").strip()
    if not pid or not Path(avatar_path).is_file():
        return False
    urls = [
        f"https://www.facebook.com/{pid}",
        f"https://www.facebook.com/profile.php?id={pid}",
        f"https://www.facebook.com/{pid}/",
    ]
    for url in urls:
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=60_000)
            _human_pause(page)
        except Exception as exc:  # noqa: BLE001
            logger.debug("Không mở được Page để gắn avatar {}: {}", url, exc)
            continue
        blocked = _surface_code(page)
        if blocked:
            _shot(page, f"avatar_{blocked.lower()}")
            return False
        _switch_into_created_page(page)
        # Thử mở menu ảnh đại diện rồi upload.
        if _attach_avatar_on_surface(page, avatar_path):
            _confirm_photo_dialog(page)
            return True
        for control in _avatar_upload_controls(page):
            try:
                control.click(timeout=4_000)
                _step_pause(page)
            except Exception:  # noqa: BLE001
                continue
            if _attach_avatar_on_surface(page, avatar_path):
                _confirm_photo_dialog(page)
                return True
    return False
