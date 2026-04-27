"""
Browser Engine — chỉ nhiệm vụ mở trình duyệt đúng profile + proxy + cookie path.

Không đọc ``pages.json`` / draft; chỉ nhận ``account_id`` + ``AccountsDatabaseManager``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Tuple

from loguru import logger
from playwright.sync_api import BrowserContext

from src.automation.browser_factory import BrowserFactory, sync_close_persistent_context

if TYPE_CHECKING:
    from src.utils.db_manager import AccountsDatabaseManager


class BrowserEngine:
    """
    Lớp mỏng quanh ``BrowserFactory``: persistent context theo tài khoản.

    Đầu ra: ``BrowserContext`` (và factory để đóng sau khi dùng xong).
    """

    def __init__(self, accounts: "AccountsDatabaseManager", *, headless: bool = True) -> None:
        self._accounts = accounts
        self._headless = headless

    def launch_context(self, account_id: str) -> Tuple[BrowserFactory, BrowserContext]:
        """
        Mở context cho ``account_id`` (profile + proxy theo accounts.json).

        Caller phải ``context.close()`` và ``factory.close()`` khi xong.
        """
        factory = BrowserFactory(accounts=self._accounts, headless=self._headless)
        ctx = factory.get_browser_context(account_id, headless=self._headless)
        return factory, ctx

    @staticmethod
    def verify_profile_ready(
        accounts: "AccountsDatabaseManager",
        account_id: str,
        *,
        headless: bool = True,
    ) -> tuple[bool, str]:
        """
        Kiểm tra nhanh: mở context và tải ``about:blank``.

        Returns:
            (True, thông báo) hoặc (False, lỗi).
        """
        if accounts.get_by_id(account_id) is None:
            return False, "Không tìm thấy tài khoản."
        factory: Optional[BrowserFactory] = None
        ctx: Optional[BrowserContext] = None
        try:
            factory = BrowserFactory(accounts=accounts, headless=headless)
            ctx = factory.get_browser_context(account_id, headless=headless)
            page = ctx.pages[0] if ctx.pages else ctx.new_page()
            page.goto("about:blank", wait_until="domcontentloaded", timeout=60_000)
            return True, "Profile + proxy khởi tạo OK (đã mở about:blank)."
        except Exception as exc:  # noqa: BLE001
            logger.exception("BrowserEngine.verify_profile_ready: {}", account_id)
            return False, str(exc)
        finally:
            sync_close_persistent_context(ctx, log_label=account_id)
            if factory is not None:
                try:
                    factory.close()
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Đóng factory sau verify: {}", exc)
