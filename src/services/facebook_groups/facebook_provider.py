"""Provider Facebook Group. Dùng profile sẵn có. Không tải video, không vượt CAPTCHA."""

from __future__ import annotations

from typing import Any
from urllib.parse import quote

from loguru import logger

from src.services.page_creation.facebook_provider import classify_facebook_surface

from .classify import (
    classify_membership_text,
    classify_posting_text,
    group_rows_from_cards,
    join_action_from_label,
    parse_group_search_html,
)


_PAUSE_TEXT = {
    "CAPTCHA": "Facebook đang hiện CAPTCHA. Xử lý trên trình duyệt rồi tìm lại.",
    "CHECKPOINT": "Facebook đang yêu cầu xác minh. Xử lý trên trình duyệt rồi tìm lại.",
    "RATE_LIMITED": "Facebook đang giới hạn thao tác. Đợi rồi tìm lại.",
    "SESSION_EXPIRED": "Phiên đăng nhập đã hết. Đăng nhập lại rồi tìm lại.",
    "SECURITY_VERIFICATION": "Facebook đang yêu cầu xác minh bảo mật.",
}


class FacebookGroupProvider:
    """Tìm nhóm và chia sẻ link qua một profile. Không mở trình duyệt mới cho từng từ khóa."""

    def __init__(self, fetch_html: Any = None) -> None:
        self._fetch_html = fetch_html
        self._account_id = ""
        self._factory: Any = None
        self._context: Any = None
        self._page: Any = None

    def check_session(self, account_id: str) -> str:
        """Rỗng nếu account còn trong kho. Hết phiên xử lý khi mở Facebook."""
        if not str(account_id or "").strip():
            return ""
        from src.utils.db_manager import AccountsDatabaseManager

        if AccountsDatabaseManager().get_by_id(account_id) is None:
            return "SESSION_EXPIRED"
        return ""

    def search_groups(
        self,
        *,
        topic: str,
        keyword: str,
        country: str,
        language: str,
        account_id: str,
        place: str = "",
    ) -> dict[str, Any]:
        query = " ".join(part for part in (keyword or topic, place) if str(part or "").strip()).strip()
        url = f"https://www.facebook.com/search/groups/?q={quote(query)}"
        if self._fetch_html is not None:
            html = self._load_html(account_id, url)
            code = self._pause_code(url, html)
            if code:
                return {"error_code": code, "groups": []}
            return {
                "error_code": "",
                "groups": parse_group_search_html(html, keyword=keyword, country=country, language=language),
            }
        page = self._page
        if page is None:
            self.begin_account(account_id)
            page = self._page
        if page is None:
            return {"error_code": "TEMPORARY_ERROR", "error_message": "Không mở được trình duyệt của account", "groups": []}
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=60_000)
            page.wait_for_timeout(1500)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Không mở được trang tìm nhóm: {}", exc)
            return {"error_code": "TEMPORARY_ERROR", "error_message": "Không mở được trang tìm nhóm", "groups": []}
        code, groups = self._scroll_group_results(page, keyword=keyword, country=country, language=language)
        if code:
            return {"error_code": code, "error_message": _PAUSE_TEXT.get(code, code), "groups": []}
        return {"error_code": "", "groups": groups}

    def check_membership(self, *, account_id: str, page_id: str, group_id: str) -> dict[str, Any]:
        html = self._load_html(account_id, f"https://www.facebook.com/groups/{group_id}")
        membership, approval = classify_membership_text(html)
        return {
            "error_code": self._surface_code(html),
            "membership_status": membership,
            "approval_status": approval,
            "posting_permission": classify_posting_text(html, membership_status=membership),
            "page_id": page_id,
        }

    def request_join_if_available(self, *, account_id: str, page_id: str, group_id: str) -> dict[str, Any]:
        """Bấm Join khi nút tham gia thẳng đang hiện. Không bấm Request to join."""
        checked = self.check_membership(account_id=account_id, page_id=page_id, group_id=group_id)
        if checked.get("error_code"):
            return checked
        if checked.get("approval_status") != "NO_APPROVAL_INDICATED":
            return checked
        if self._page is None:
            return checked
        action = self._click_join(self._page)
        if action == "approval":
            checked["approval_status"] = "APPROVAL_REQUIRED"
            checked["membership_status"] = "NOT_JOINED"
            return checked
        if action != "join":
            checked["error_code"] = "TEMPORARY_ERROR"
            checked["error_message"] = "Chưa thấy nút tham gia thẳng"
            return checked
        return self.check_membership(account_id=account_id, page_id=page_id, group_id=group_id)

    def verify_membership(self, *, account_id: str, page_id: str, group_id: str) -> dict[str, Any]:
        return self.check_membership(account_id=account_id, page_id=page_id, group_id=group_id)

    def browse_reels(
        self,
        *,
        account_id: str,
        reel_seconds: int = 0,
        should_stop: Any = None,
    ) -> dict[str, Any]:
        """Mở bảng Reel và lướt đủ số giây. Không bấm thích, bình luận hay chia sẻ."""
        from src.services.facebook_groups.video_engage import browse_reels_feed

        seconds = int(reel_seconds or 0)
        if seconds <= 0:
            return {"error_code": "", "browsed_seconds": 0}
        if self._fetch_html is not None:
            return {"error_code": "", "browsed_seconds": seconds}
        self._load_html(account_id, "https://www.facebook.com/reels/")
        if self._page is None:
            return {"error_code": "TIMEOUT", "browsed_seconds": 0}
        code = self._pause_code(str(getattr(self._page, "url", "") or ""), self._visible_text(self._page))
        if code:
            return {"error_code": code, "browsed_seconds": 0}
        return browse_reels_feed(self._page, seconds=seconds, should_stop=should_stop)

    def prepare_source_video(
        self,
        *,
        account_id: str,
        source_url: str,
        watch_seconds: int = 0,
        comment: str = "",
        should_stop: Any = None,
    ) -> dict[str, Any]:
        """Mở link video, xem đủ số giây, rồi bình luận. Không tải file."""
        from src.services.facebook_groups.video_engage import engage_source_video

        if not source_url:
            return {"error_code": "TEMPORARY_ERROR", "error_message": "Thiếu link video"}
        if self._fetch_html is not None:
            return {
                "error_code": "",
                "watched_seconds": int(watch_seconds or 0),
                "commented": bool(str(comment or "").strip()),
            }
        self._load_html(account_id, source_url)
        if self._page is None:
            return {"error_code": "TIMEOUT", "watched_seconds": 0, "commented": False}
        code = self._pause_code(str(getattr(self._page, "url", "") or ""), self._visible_text(self._page))
        if code:
            return {"error_code": code, "watched_seconds": 0, "commented": False}
        return engage_source_video(
            self._page,
            watch_seconds=int(watch_seconds or 0),
            comment=str(comment or ""),
            should_stop=should_stop,
        )

    def publish_link(
        self,
        *,
        account_id: str,
        page_id: str,
        group_id: str,
        source_url: str,
        text: str,
        image_paths: list[str] | None = None,
        target_url: str = "",
        page_name: str = "",
        group_name: str = "",
    ) -> dict[str, Any]:
        """Bấm nút chia sẻ trên video nguồn, rồi gửi lên Page hoặc nhóm trong list."""
        if not source_url:
            return {"error_code": "TEMPORARY_ERROR", "error_message": "Thiếu URL"}
        if self._page is not None and source_url not in str(getattr(self._page, "url", "") or ""):
            self._load_html(account_id, source_url)
        if self._fetch_html is None and self._page is not None:
            from src.services.facebook_groups.video_engage import share_open_video

            code = self._pause_code(str(getattr(self._page, "url", "") or ""), self._visible_text(self._page))
            if code:
                return {"error_code": code}
            shared = share_open_video(
                self._page,
                kind="group" if group_id else "page",
                target_id=group_id or page_id,
                target_name=group_name if group_id else page_name,
                caption=text,
            )
            if shared.get("error_code"):
                return shared
            return {"post_id": str(shared.get("post_id") or ""), "post_url": source_url}
        url = target_url.strip() if not group_id else ""
        if not url:
            url = f"https://www.facebook.com/groups/{group_id}" if group_id else f"https://www.facebook.com/{page_id}"
        html = self._load_html(account_id, url)
        code = self._surface_code(html)
        if code:
            return {"error_code": code}
        if self._page is None:
            return {"error_code": "TIMEOUT", "post_id": ""}
        posted = self._type_link_post(self._page, text, source_url, list(image_paths or []))
        if not posted:
            return {"error_code": "TIMEOUT", "post_id": ""}
        if source_url in (self._page.content() or ""):
            return {"post_id": f"{group_id}-link", "post_url": source_url}
        return {"error_code": "TIMEOUT", "post_id": ""}

    def verify_published_post(
        self,
        *,
        account_id: str,
        page_id: str,
        group_id: str,
        source_url: str,
        text: str,
    ) -> dict[str, Any]:
        del page_id, text
        html = self._load_html(account_id, f"https://www.facebook.com/groups/{group_id}")
        code = self._surface_code(html)
        if code:
            return {"error_code": code, "post_id": ""}
        if source_url and source_url in html:
            return {"post_id": f"{group_id}-link", "post_url": source_url}
        return {"error_code": "TIMEOUT", "post_id": ""}

    def begin_account(self, account_id: str) -> None:
        """Mở một profile cho cả lượt tìm, tham gia hoặc chia sẻ."""
        if self._fetch_html is not None or not str(account_id or "").strip():
            return
        if self._page is not None and self._account_id == account_id:
            return
        self.end_account(account_id)
        self._open_browser(account_id)

    def end_account(self, account_id: str) -> None:
        """Đóng profile sau khi hàng đợi của account đó xong."""
        del account_id
        context = self._context
        factory = self._factory
        self._page = None
        self._context = None
        self._factory = None
        self._account_id = ""
        if context is None and factory is None:
            return
        from src.automation.browser_factory import sync_close_persistent_context

        sync_close_persistent_context(context, log_label="group", same_thread=True)
        if factory is not None:
            try:
                factory.close()
            except Exception:  # noqa: BLE001
                pass

    def _scroll_group_results(
        self,
        page: Any,
        *,
        keyword: str,
        country: str,
        language: str,
        max_steps: int = 40,
    ) -> tuple[str, list[dict[str, Any]]]:
        """Cuộn trang tìm cho đến khi không còn nhóm mới. Gom link trước khi Facebook gỡ khỏi DOM."""
        seen: dict[str, dict[str, Any]] = {}
        idle = 0
        opened_tab = False
        for _step in range(max_steps):
            html = ""
            try:
                html = page.content() or ""
            except Exception as exc:  # noqa: BLE001
                logger.debug("Chưa đọc được kết quả tìm nhóm: {}", exc)
            code = self._pause_code(getattr(page, "url", "") or "", self._visible_text(page))
            if code:
                return code, []
            if not seen and not opened_tab:
                opened_tab = True
                self._focus_groups_tab(page)
            before = len(seen)
            for row in parse_group_search_html(html, keyword=keyword, country=country, language=language):
                self._remember_group(seen, row)
            for row in group_rows_from_cards(
                self._read_group_cards(page),
                keyword=keyword,
                country=country,
                language=language,
            ):
                self._remember_group(seen, row)
            if len(seen) == before:
                idle += 1
            else:
                idle = 0
                logger.info("Đã quét {} nhóm cho từ khóa {}.", len(seen), keyword or country or "tìm kiếm")
            stop_after = 3 if seen else 8
            if idle >= stop_after:
                break
            self._click_load_more(page)
            try:
                page.evaluate("window.scrollBy(0, Math.max(window.innerHeight || 800, 900))")
                page.wait_for_timeout(1100)
            except Exception:  # noqa: BLE001
                break
        return "", list(seen.values())

    def _remember_group(self, seen: dict[str, dict[str, Any]], row: dict[str, Any]) -> None:
        group_id = str(row.get("group_id") or "")
        if not group_id:
            return
        current = seen.get(group_id)
        if current is None:
            seen[group_id] = row
            return
        if int(row.get("member_count") or 0) > int(current.get("member_count") or 0):
            current["member_count"] = row.get("member_count")
        if row.get("group_name") and current.get("group_name") == group_id:
            current["group_name"] = row.get("group_name")

    def _visible_text(self, page: Any) -> str:
        """Chỉ chữ trên màn hình. Không đọc mã nguồn, để khỏi nhận nhầm chữ captcha trong script."""
        reader = getattr(page, "inner_text", None)
        if reader is None:
            return ""
        try:
            return str(reader("body") or "")
        except Exception:  # noqa: BLE001
            return ""

    def _read_group_cards(self, page: Any) -> list[dict[str, Any]]:
        try:
            cards = page.evaluate(
                """() => Array.from(document.querySelectorAll('a[href*="/groups/"]')).map((node) => {
                    let text = "";
                    let parent = node;
                    for (let depth = 0; depth < 8 && parent; depth += 1) {
                        const sample = (parent.innerText || "").trim();
                        if (/members|thành viên|thanh vien/i.test(sample) && sample.length < 500) {
                            text = sample;
                            break;
                        }
                        parent = parent.parentElement;
                    }
                    return {href: node.href || "", name: node.innerText || node.getAttribute("aria-label") || "", text};
                })"""
            )
        except Exception:  # noqa: BLE001
            return []
        return cards if isinstance(cards, list) else []

    def _click_load_more(self, page: Any) -> None:
        """Bấm tải thêm nếu trang tìm còn trang sau. Không vượt CAPTCHA."""
        getter = getattr(page, "get_by_role", None)
        if getter is None:
            return
        for label in ("See more", "Load more", "Xem thêm"):
            try:
                button = getter("button", name=label)
                if button.count() and button.first.is_visible():
                    button.first.click(timeout=3_000)
                    page.wait_for_timeout(1000)
                    return
            except Exception:  # noqa: BLE001
                continue

    def _focus_groups_tab(self, page: Any) -> None:
        getter = getattr(page, "get_by_role", None)
        if getter is None:
            return
        try:
            tab = getter("tab", name="Groups")
            if tab.count() == 0:
                tab = getter("link", name="Groups")
            if tab.count() == 0:
                tab = getter("tab", name="Nhóm")
            if tab.count():
                tab.first.click(timeout=3_000)
                page.wait_for_timeout(1200)
        except Exception:  # noqa: BLE001
            return

    def _pause_code(self, url: str, html: str) -> str:
        code = classify_facebook_surface(url, html)
        mapping = {
            "CAPTCHA_DETECTED": "CAPTCHA",
            "CHECKPOINT": "CHECKPOINT",
            "RATE_LIMITED": "RATE_LIMITED",
            "SESSION_EXPIRED": "SESSION_EXPIRED",
            "SMS_VERIFICATION_REQUIRED": "SECURITY_VERIFICATION",
            "SECURITY_VERIFICATION_REQUIRED": "SECURITY_VERIFICATION",
        }
        return mapping.get(code, "")

    def _load_html(self, account_id: str, url: str) -> str:
        """Đọc HTML trên profile đang mở. Test tiêm HTML thì không mở trình duyệt."""
        if self._fetch_html is not None:
            return str(self._fetch_html(account_id, url) or "")
        page = self._page
        if page is None:
            self.begin_account(account_id)
            page = self._page
        if page is None:
            return ""
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=60_000)
            page.wait_for_timeout(1200)
            return page.content() or ""
        except Exception as exc:  # noqa: BLE001
            logger.warning("Không mở được {}: {}", url, exc)
            return ""

    def _open_browser(self, account_id: str) -> None:
        from src.automation.browser_factory import BrowserFactory, prepare_playwright_sync_thread
        from src.utils.account_proxy_mapper import prepare_account_dict_for_browser_run
        from src.utils.db_manager import AccountsDatabaseManager

        row = AccountsDatabaseManager().get_by_id(account_id)
        if row is None:
            return
        prepare_playwright_sync_thread(label=f"group:{account_id}")
        acc = prepare_account_dict_for_browser_run(dict(row), require_proxy_live=True)
        factory = BrowserFactory(headless=False, playwright_shared=True)
        context = factory.launch_persistent_context_from_account_dict(
            acc,
            headless=False,
            disable_notifications=True,
            force_desktop_facebook=True,
        )
        self._factory = factory
        self._context = context
        self._page = context.pages[0] if context.pages else context.new_page()
        self._account_id = account_id

    def _click_join(self, page: Any) -> str:
        try:
            buttons = page.get_by_role("button")
            count = min(buttons.count(), 30)
        except Exception:  # noqa: BLE001
            return ""
        for index in range(count):
            item = buttons.nth(index)
            try:
                if not item.is_visible():
                    continue
                label = item.inner_text(timeout=500) or ""
            except Exception:  # noqa: BLE001
                continue
            action = join_action_from_label(label)
            if action == "approval":
                return "approval"
            if action == "join":
                item.click(timeout=5_000)
                page.wait_for_timeout(1500)
                return "join"
        return ""

    def _type_link_post(self, page: Any, text: str, source_url: str, image_paths: list[str] | None = None) -> bool:
        """Gõ nội dung, dán link bài và gắn ảnh local. Không chọn file video."""
        body = f"{text.strip()}\n{source_url}".strip()
        if image_paths and not self._attach_local_images(page, image_paths):
            return False
        try:
            boxes = page.get_by_role("textbox")
            count = min(boxes.count(), 6)
        except Exception:  # noqa: BLE001
            return False
        target = None
        for index in range(count):
            item = boxes.nth(index)
            try:
                if item.is_visible():
                    target = item
                    break
            except Exception:  # noqa: BLE001
                continue
        if target is None:
            return False
        try:
            target.click(timeout=4_000)
            target.type(body, delay=25)
            page.wait_for_timeout(1500)
            post = page.get_by_role("button", name="Post")
            if post.count() == 0:
                post = page.get_by_role("button", name="Đăng")
            if post.count() and post.first.is_enabled():
                post.first.click(timeout=5_000)
                page.wait_for_timeout(1500)
                return True
        except Exception as exc:  # noqa: BLE001
            logger.debug("Chưa gửi được link vào nhóm: {}", exc)
        return False

    def _attach_local_images(self, page: Any, paths: list[str]) -> bool:
        """Gắn ảnh có sẵn trên máy. Không tải ảnh từ link bài."""
        try:
            picker = page.locator("input[type='file']")
            if picker.count() == 0:
                return False
            picker.first.set_input_files(paths)
            page.wait_for_timeout(1200)
            return True
        except Exception as exc:  # noqa: BLE001
            logger.debug("Chưa gắn được ảnh: {}", exc)
            return False

    def _surface_code(self, html: str) -> str:
        code = classify_facebook_surface("", html)
        mapping = {
            "CAPTCHA_DETECTED": "CAPTCHA",
            "CHECKPOINT": "CHECKPOINT",
            "RATE_LIMITED": "RATE_LIMITED",
            "SESSION_EXPIRED": "SESSION_EXPIRED",
            "SMS_VERIFICATION_REQUIRED": "SECURITY_VERIFICATION",
        }
        return mapping.get(code, "")
