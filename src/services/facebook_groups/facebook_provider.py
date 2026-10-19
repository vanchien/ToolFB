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


def _share_page_url(page_id: str, page_url: str) -> str:
    """Link Page đã lưu. Không đổi sang dạng khác kẻo Facebook báo không xem được."""
    dest = str(page_url or "").strip()
    if dest:
        return dest
    token = str(page_id or "").strip()
    if token:
        return f"https://www.facebook.com/{token}"
    return ""


class FacebookGroupProvider:
    """Tìm nhóm và chia sẻ link qua một profile. Không mở trình duyệt mới cho từng từ khóa."""

    def __init__(self, fetch_html: Any = None) -> None:
        self._fetch_html = fetch_html
        self._account_id = ""
        self._factory: Any = None
        self._context: Any = None
        self._page: Any = None

    def spawn(self) -> FacebookGroupProvider:
        """Một profile riêng. Nhiều tài khoản chia sẻ cùng lúc thì mỗi tài khoản một cửa sổ."""
        return FacebookGroupProvider(self._fetch_html)

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

    def switch_to_page(
        self,
        *,
        account_id: str,
        page_id: str,
        page_name: str = "",
        page_url: str = "",
    ) -> dict[str, Any]:
        """Chuyển vai trò sang đúng Page trước khi lướt Reel, xem, like, bình luận và chia sẻ.

        Mở trang Page đích trước (nút «Switch now» chỉ hiện ở đó), rồi mới switch. Lỗi thì
        về trang chủ cá nhân và thử lại lần 2. Vẫn lỗi thì để trình duyệt ở trang chủ sạch
        để Page sau không bị kẹt theo.
        """
        if self._fetch_html is not None:
            return {"error_code": "", "switched": True}
        if not self.ensure_browser(account_id):
            return {"error_code": "TIMEOUT", "error_message": "Không mở được trình duyệt"}
        from src.automation.facebook_actions import (
            _disable_view_only_guard,
            _dismiss_open_profile_menu,
            _enable_view_only_guard,
            _go_facebook_personal_home,
            _robust_switch_to_target_page,
            _target_page_role_satisfied,
            navigate_to_url,
        )

        dest = _share_page_url(page_id, page_url)
        name = str(page_name or "").strip()
        label = name or page_id
        if not dest:
            return {"error_code": "TEMPORARY_ERROR", "error_message": f"Thiếu link Page {label}"}

        def _closed(exc: BaseException) -> bool:
            msg = str(exc or "").casefold()
            return "has been closed" in msg or "target page" in msg or "context or browser" in msg

        ok = False
        attempts = 3
        for attempt in range(1, attempts + 1):
            if not self.ensure_browser(account_id):
                return {"error_code": "TIMEOUT", "error_message": "Không mở được trình duyệt"}
            page = self._page
            if page is None:
                return {"error_code": "TIMEOUT", "error_message": "Không mở được trình duyệt"}
            _disable_view_only_guard(page)
            try:
                try:
                    if attempt >= 2:
                        logger.info(
                            "Chuyển Page {} lần {} — về trang chủ rồi thử lại.",
                            label,
                            attempt,
                        )
                        _dismiss_open_profile_menu(page)
                        _go_facebook_personal_home(page)
                    navigate_to_url(page, dest)
                    page.wait_for_timeout(1_500)
                    if _robust_switch_to_target_page(page, page_display_name=name, page_url=dest):
                        _dismiss_open_profile_menu(page)
                        ok = _target_page_role_satisfied(
                            page, dest, timeout_ms=4_000, page_display_name=name
                        )
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Chuyển Page {} lần {} lỗi: {}", label, attempt, exc)
                    ok = False
                    if _closed(exc):
                        # Xóa page chết để ensure_browser mở lại vòng sau.
                        self._page = None
                        continue
                if ok:
                    break
            finally:
                if self._page_is_alive():
                    try:
                        _enable_view_only_guard(self._page, force=True)
                    except Exception:  # noqa: BLE001
                        pass
        if not ok:
            try:
                if self._page_is_alive():
                    _dismiss_open_profile_menu(self._page)
                    _go_facebook_personal_home(self._page)
            except Exception:  # noqa: BLE001
                pass
            return {
                "error_code": "TEMPORARY_ERROR",
                "error_message": f"Chưa chuyển được sang Page {label} (đã thử {attempts} lần)",
            }
        logger.info("Đã chuyển sang Page {}.", label)
        return {"error_code": "", "switched": True}

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

    def like_source_video(self, *, account_id: str, source_url: str) -> dict[str, Any]:
        """Bấm Like trên video đang mở. Không bấm React."""
        from src.services.facebook_groups.video_engage import like_open_video

        if self._fetch_html is not None:
            return {"error_code": "", "liked": True}
        if not source_url:
            return {"error_code": "TEMPORARY_ERROR", "liked": False}
        if self._page is None or source_url not in str(getattr(self._page, "url", "") or ""):
            self._load_html(account_id, source_url)
        if self._page is None:
            return {"error_code": "TIMEOUT", "liked": False}
        code = self._pause_code(str(getattr(self._page, "url", "") or ""), self._visible_text(self._page))
        if code:
            return {"error_code": code, "liked": False}
        return {"error_code": "", "liked": bool(like_open_video(self._page))}

    def prepare_source_video(
        self,
        *,
        account_id: str,
        source_url: str,
        watch_seconds: int = 0,
        comment: str = "",
        like: bool = False,
        should_stop: Any = None,
    ) -> dict[str, Any]:
        """Mở link video, xem đủ số giây; trong lúc xem thì Like và bình luận lần lượt. Không tải file."""
        from src.services.facebook_groups.video_engage import engage_source_video

        if not source_url:
            return {"error_code": "TEMPORARY_ERROR", "error_message": "Thiếu link video"}
        if self._fetch_html is not None:
            return {
                "error_code": "",
                "watched_seconds": int(watch_seconds or 0),
                "commented": bool(str(comment or "").strip()),
                "liked": bool(like),
            }
        self._load_html(account_id, source_url)
        if self._page is None:
            return {"error_code": "TIMEOUT", "watched_seconds": 0, "commented": False, "liked": False}
        code = self._pause_code(str(getattr(self._page, "url", "") or ""), self._visible_text(self._page))
        if code:
            return {"error_code": code, "watched_seconds": 0, "commented": False, "liked": False}
        return engage_source_video(
            self._page,
            watch_seconds=int(watch_seconds or 0),
            comment=str(comment or ""),
            like=bool(like),
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
        like_video: bool = False,
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
            share_args = {
                "kind": "group" if group_id else "page",
                "target_id": group_id or page_id,
                "target_name": group_name if group_id else page_name,
                "caption": text,
                "like": bool(like_video),
            }
            shared = share_open_video(self._page, **share_args)
            if shared.get("error_code") == "TIMEOUT" and "nút chia sẻ" in str(shared.get("error_message") or ""):
                logger.info("Chưa thấy nút chia sẻ — tải lại video rồi thử lại một lần.")
                try:
                    self._page.goto(source_url, wait_until="domcontentloaded", timeout=60_000)
                    self._page.wait_for_timeout(2_000)
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Không tải lại được video: {}", exc)
                else:
                    shared = share_open_video(self._page, **{**share_args, "like": False})
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

    def _page_is_alive(self) -> bool:
        """True nếu page Playwright còn dùng được (chưa đóng context/browser)."""
        page = self._page
        if page is None:
            return False
        try:
            _ = page.url
            return True
        except Exception:  # noqa: BLE001
            return False

    def ensure_browser(self, account_id: str) -> bool:
        """
        Đảm bảo đang có trình duyệt sống cho account.

        Giữa lượt chia sẻ Firefox đôi khi đóng — nếu vẫn giữ page chết thì
        mọi Page sau đều lỗi «has been closed» trong tích tắc.
        """
        if self._fetch_html is not None:
            return True
        token = str(account_id or "").strip()
        if not token:
            return False
        if self._page_is_alive() and self._account_id == token:
            return True
        logger.warning(
            "Trình duyệt account {} đã đóng hoặc lệch session — mở lại để tiếp tục.",
            token,
        )
        try:
            self.end_account(token, force_kill_firefox=True)
        except Exception:  # noqa: BLE001
            self._page = None
            self._context = None
            self._factory = None
            self._account_id = ""
            self._kill_stale_firefox(token)
        self._open_browser(token)
        return self._page_is_alive()

    def begin_account(self, account_id: str) -> None:
        """Mở một profile cho cả lượt tìm, tham gia hoặc chia sẻ."""
        if self._fetch_html is not None or not str(account_id or "").strip():
            return
        # Page chết vẫn khác None — phải mở lại, không return sớm.
        if self._page_is_alive() and self._account_id == account_id:
            return
        # Đóng context cũ + ép kill firefox còn sót (tránh 2 cửa sổ cùng profile).
        self.end_account(account_id, force_kill_firefox=True)
        self._open_browser(account_id)

    def end_account(self, account_id: str, *, force_kill_firefox: bool = False) -> None:
        """Đóng profile sau khi hàng đợi của account đó xong."""
        token = str(account_id or self._account_id or "").strip()
        context = self._context
        factory = self._factory
        self._page = None
        self._context = None
        self._factory = None
        self._account_id = ""
        if context is None and factory is None:
            if force_kill_firefox and token:
                self._kill_stale_firefox(token)
            return
        from src.automation.browser_factory import sync_close_persistent_context

        sync_close_persistent_context(
            context,
            log_label="group",
            same_thread=True,
            force_kill_firefox=force_kill_firefox,
        )
        if factory is not None:
            try:
                factory.close()
            except Exception:  # noqa: BLE001
                pass
        if force_kill_firefox and token:
            self._kill_stale_firefox(token)

    @staticmethod
    def _kill_stale_firefox(account_id: str) -> None:
        """Ép đóng firefox.exe còn giữ portable_path của TK (tránh mở trùng cửa sổ)."""
        try:
            from src.utils.account_browser_profile import resolve_account_path
            from src.utils.db_manager import AccountsDatabaseManager
            from src.utils.paths import project_root
            from src.utils.win_browser_window import terminate_firefox_for_profile

            row = AccountsDatabaseManager().get_by_id(account_id)
            if not row:
                return
            raw = str(row.get("portable_path") or row.get("profile_path") or "").strip()
            folder = resolve_account_path(project_root(), raw) if raw else None
            if folder is not None and folder.is_dir():
                terminate_firefox_for_profile(folder, grace_ms=200)
        except Exception as exc:  # noqa: BLE001
            logger.debug("kill stale firefox {}: {}", account_id, exc)

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
            current = str(getattr(page, "url", "") or "")
            from src.services.facebook_groups.video_engage import _lock_user

            if url and (current == url or url in current):
                _lock_user(page)
                return page.content() or ""
            page.goto(url, wait_until="domcontentloaded", timeout=60_000)
            page.wait_for_timeout(800)
            _lock_user(page)
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
        # Dọn firefox sót trước khi launch — tránh 2 cửa sổ cùng profile.
        self._kill_stale_firefox(account_id)
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
        # Chỉ giữ 1 page lúc mở — đóng tab thừa từ session cũ (không auto-đóng popup Switch).
        self._page = self._keep_single_page(context)
        self._account_id = account_id
        from src.services.facebook_groups.video_engage import _lock_user

        _lock_user(self._page)

    @staticmethod
    def _keep_single_page(context: Any) -> Any:
        """Giữ pages[0], đóng mọi tab còn lại khi vừa launch."""
        pages = list(context.pages) if context.pages else []
        if not pages:
            return context.new_page()
        main = pages[0]
        for extra in pages[1:]:
            try:
                extra.close()
            except Exception:  # noqa: BLE001
                pass
        return main

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
