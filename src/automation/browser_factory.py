"""
Khởi tạo Playwright persistent context theo từng tài khoản: profile portable + proxy + stealth.

Đọc cấu hình từ config/accounts.json thông qua AccountsDatabaseManager (không hard-code secrets).
"""

from __future__ import annotations

import os
import sys
import threading
import time
from pathlib import Path
from typing import Any, Literal, Optional
from urllib.parse import quote

from loguru import logger
from playwright.sync_api import BrowserContext, Page, Playwright, sync_playwright
try:
    from playwright_stealth import Stealth  # type: ignore
except Exception as _stealth_import_exc:  # noqa: BLE001
    Stealth = None  # type: ignore[assignment]
    _STEALTH_IMPORT_ERROR = _stealth_import_exc
else:
    _STEALTH_IMPORT_ERROR = None

from src.automation.mobile_viewport import resolve_mobile_viewport
from src.utils.browser_fingerprint import (
    apply_fingerprint_init_script,
    default_desktop_firefox_user_agent,
    merge_firefox_user_prefs,
    resolve_browser_locale,
    resolve_browser_timezone,
    resolve_proxy_geo,
)
from src.utils.proxy_check import proxy_host_port_configured
from src.utils.db_manager import AccountRecord, AccountsDatabaseManager, ProxyConfig
from src.utils.paths import project_root as _project_root


def _stealth_for_project(*, use_mobile_fingerprint: bool = False) -> Any:
    """
    Stealth đầy đủ cho desktop; bản rút gọn khi dùng fingerprint mobile (viewport hẹp / FB_MOBILE_MODE).

    ``use_mobile_fingerprint``: bật khi ``FB_MOBILE_MODE`` hoặc ``use_mobile_facebook_shell`` từ viewport.
    ``FB_STEALTH_FULL=1``: luôn Stealth đầy đủ.

    Các override tắt rõ ``None`` để tránh UserWarning của playwright-stealth.
    """
    if Stealth is None:
        return None
    if not use_mobile_fingerprint or _env_bool("FB_STEALTH_FULL", False):
        return Stealth()
    return Stealth(
        navigator_platform=False,
        navigator_platform_override=None,
        navigator_user_agent=False,
        navigator_user_agent_override=None,
        navigator_user_agent_data=False,
        sec_ch_ua=False,
        sec_ch_ua_override=None,
        navigator_vendor=False,
        navigator_vendor_override=None,
        navigator_plugins=False,
        chrome_app=False,
        chrome_csi=False,
        chrome_load_times=False,
        hairline=False,
    )


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    return raw.lower() not in {"0", "false", "off", "no"}


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(float(raw))
    except (TypeError, ValueError):
        return default


def _append_unique_arg(args: list[str], arg: str) -> None:
    """Thêm launch arg nếu chưa có để tránh trùng lặp."""
    if not arg or arg in args:
        return
    args.append(arg)


_PLAYWRIGHT_CHROMIUM_CHANNELS_OK = frozenset(
    {"chrome", "chrome-beta", "chrome-dev", "msedge", "msedge-beta", "msedge-dev"}
)


def _playwright_chromium_channel(*, browser_key: str, has_executable_path: bool) -> str | None:
    """
    Chọn ``channel`` cho Chromium/Chrome (Playwright): dùng Chrome/Edge cài trên máy thay cho bundle.

    Mặc định ``chrome`` (ổn định hơn, tích hợp cert/policy Windows). Tắt → bundle Playwright:
    ``FB_PLAYWRIGHT_CHROMIUM_CHANNEL=bundled`` (hoặc ``0`` / ``false``).

    Không áp dụng khi đã có ``browser_exe_path`` hợp lệ trên account.
    """
    if browser_key not in ("chromium", "chrome") or has_executable_path:
        return None
    if getattr(sys, "frozen", False) or _env_bool("TOOLFB_ENFORCE_BUNDLED_BROWSER", False):
        return None
    raw_env = os.environ.get("FB_PLAYWRIGHT_CHROMIUM_CHANNEL")
    raw = ("chrome" if raw_env is None else str(raw_env)).strip().lower()
    if not raw:
        # Nếu người dùng để rỗng, vẫn ưu tiên Chrome hệ thống để tốc độ/độ ổn định tốt hơn.
        raw = "chrome"
    if not raw or raw in ("0", "false", "off", "no", "bundled", "bundle", "playwright", "ms-playwright", "chromium"):
        return None
    if raw in _PLAYWRIGHT_CHROMIUM_CHANNELS_OK:
        return raw
    logger.warning(
        "FB_PLAYWRIGHT_CHROMIUM_CHANNEL={!r} không hỗ trợ — dùng Chromium bundle. Hợp lệ: {}.",
        raw,
        ", ".join(sorted(_PLAYWRIGHT_CHROMIUM_CHANNELS_OK)),
    )
    return None


def _sleep_before_playwright_driver_stop() -> None:
    """
    Chờ ngắn trước khi gọi ``playwright.stop()`` để driver Node kịp xả IPC.

    Giảm lỗi ``EPIPE: broken pipe`` trên Windows khi đóng browser/context liên tục.
    Tắt: ``FB_PLAYWRIGHT_STOP_GRACE_MS=0``.
    """
    ms = _env_int("FB_PLAYWRIGHT_STOP_GRACE_MS", 150)
    if ms <= 0:
        return
    time.sleep(ms / 1000.0)


# Một process Playwright cho mỗi worker-thread — tránh ``start()`` lặp mà vẫn không vi phạm
# ràng buộc thread-affinity của Playwright Sync API / greenlet.
_shared_pw_by_thread: dict[int, Playwright] = {}
_shared_pw_ref_by_thread: dict[int, int] = {}
_shared_pw_lock = threading.Lock()

# Một profile portable chỉ được gắn một Firefox/Playwright context tại một thời điểm.
_profile_path_locks: dict[str, threading.Lock] = {}
_profile_path_holders: dict[str, str] = {}  # profile_key -> account_id
_profile_registry_lock = threading.Lock()

_BROWSER_LAUNCH_SEM: threading.Semaphore | None = None
_BROWSER_LAUNCH_SEM_LIMIT = 0


def _browser_launch_semaphore() -> threading.Semaphore:
    """Giới hạn số Firefox/Chromium launch song song — tránh spike CPU/RAM khi pool N luồng."""
    global _BROWSER_LAUNCH_SEM, _BROWSER_LAUNCH_SEM_LIMIT
    limit = max(1, _env_int("FB_MAX_PARALLEL_BROWSER_LAUNCH", 2))
    if _BROWSER_LAUNCH_SEM is None or _BROWSER_LAUNCH_SEM_LIMIT != limit:
        _BROWSER_LAUNCH_SEM = threading.Semaphore(limit)
        _BROWSER_LAUNCH_SEM_LIMIT = limit
        logger.debug("[Browser] Semaphore launch song song tối đa {} cửa sổ.", limit)
    return _BROWSER_LAUNCH_SEM


def _acquire_browser_launch_slot(
    *,
    account_id: str = "",
    timeout_sec: float | None = None,
) -> None:
    """
    Chờ slot launch (tránh mở quá nhiều Firefox cùng lúc).

    Raises:
        RuntimeError: Hết thời gian chờ (pool khác đang giữ slot).
    """
    sem = _browser_launch_semaphore()
    if timeout_sec is None:
        sem.acquire()
        return
    wait = max(5.0, float(timeout_sec))
    if sem.acquire(timeout=wait):
        return
    aid = str(account_id or "").strip()
    raise RuntimeError(
        f"Quá {wait:.0f}s chờ mở trình duyệt"
        + (f" (account={aid})" if aid else "")
        + " — có thể đang chạy «Đăng nhập»/pool khác. Bấm «Dừng» hoặc đợi cửa sổ kia mở xong."
    )


def _profile_path_key(user_data_dir: Path) -> str:
    return str(user_data_dir.resolve()).lower()


def _get_profile_path_lock(profile_key: str) -> threading.Lock:
    with _profile_registry_lock:
        lock = _profile_path_locks.get(profile_key)
        if lock is None:
            lock = threading.Lock()
            _profile_path_locks[profile_key] = lock
        return lock


def acquire_profile_browser_slot(
    user_data_dir: Path,
    account_id: str,
    *,
    timeout_sec: float | None = None,
) -> threading.Lock:
    """
  Chiếm slot profile: mỗi ``user_data_dir`` chỉ một tài khoản / một context Firefox.

    Raises:
        RuntimeError: Profile đang mở cho tài khoản khác hoặc hết thời gian chờ khóa.
    """
    profile_key = _profile_path_key(user_data_dir)
    aid = str(account_id or "").strip() or "(unknown)"
    with _profile_registry_lock:
        holder = _profile_path_holders.get(profile_key)
        if holder and holder != aid:
            raise RuntimeError(
                f"Profile Firefox đang dùng bởi tài khoản «{holder}» "
                f"({user_data_dir}) — không mở thêm cho «{aid}». "
                "Đóng trình duyệt tài khoản kia trước."
            )
        _profile_path_holders[profile_key] = aid
    lock = _get_profile_path_lock(profile_key)
    if timeout_sec is not None:
        wait = max(5.0, float(timeout_sec))
        if not lock.acquire(timeout=wait):
            with _profile_registry_lock:
                if _profile_path_holders.get(profile_key) == aid:
                    _profile_path_holders.pop(profile_key, None)
            raise RuntimeError(
                f"Quá {wait:.0f}s chờ khóa profile Firefox (account={aid}). "
                "Có thể pool «Đăng nhập» đang giữ profile — bấm «Dừng» hoặc đóng Firefox cũ."
            )
    else:
        lock.acquire()
    logger.info(
        "Đã khóa profile Firefox | account={} | profile={}",
        aid,
        user_data_dir,
    )
    return lock


def release_profile_browser_slot(
    user_data_dir: Path, account_id: str, profile_lock: threading.Lock | None
) -> None:
    """Giải phóng slot profile sau ``context.close()``."""
    if profile_lock is None:
        return
    profile_key = _profile_path_key(user_data_dir)
    aid = str(account_id or "").strip() or "(unknown)"
    try:
        profile_lock.release()
    except RuntimeError:
        pass
    with _profile_registry_lock:
        if _profile_path_holders.get(profile_key) == aid:
            _profile_path_holders.pop(profile_key, None)
    logger.info(
        "Đã mở khóa profile Firefox | account={} | profile={}",
        aid,
        user_data_dir,
    )


def _acquire_shared_playwright() -> Playwright:
    thread_id = threading.get_ident()
    with _shared_pw_lock:
        pw = _shared_pw_by_thread.get(thread_id)
        if pw is None:
            prepare_playwright_sync_thread(label="shared_acquire")
            pw = sync_playwright().start()
            _shared_pw_by_thread[thread_id] = pw
            _shared_pw_ref_by_thread[thread_id] = 0
            logger.info(
                "Playwright đã khởi chạy (thread_id={} — chia sẻ trong cùng worker thread).",
                thread_id,
            )
        _shared_pw_ref_by_thread[thread_id] = _shared_pw_ref_by_thread.get(thread_id, 0) + 1
        return pw


def prepare_playwright_sync_thread(*, label: str = "") -> None:
    """
    Chuẩn bị thread hiện tại cho Playwright Sync API.

    Playwright từ chối chạy nếu asyncio event loop đang *running* trên thread này
    (thường do đóng browser sai thread hoặc Python 3.12+).
    """
    import asyncio

    tag = f" ({label})" if label else ""
    try:
        asyncio.get_running_loop()
        logger.warning(
            "[Playwright] Thread {}{} có asyncio loop đang chạy — thay loop mới",
            threading.get_ident(),
            tag,
        )
        asyncio.set_event_loop(asyncio.new_event_loop())
        return
    except RuntimeError:
        pass
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            asyncio.set_event_loop(asyncio.new_event_loop())
            return
        if not loop.is_running():
            loop.close()
            asyncio.set_event_loop(asyncio.new_event_loop())
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())


def _release_shared_playwright() -> None:
    thread_id = threading.get_ident()
    with _shared_pw_lock:
        ref = _shared_pw_ref_by_thread.get(thread_id, 0)
        if ref <= 0:
            logger.warning(
                "Gọi release Playwright khi refcount đã 0 (thread_id={}) — bỏ qua.",
                thread_id,
            )
            return
        ref -= 1
        _shared_pw_ref_by_thread[thread_id] = ref
        pw = _shared_pw_by_thread.get(thread_id)
        if ref == 0 and pw is not None:
            logger.info("Đang dừng Playwright (thread_id={}, refcount=0).", thread_id)
            try:
                _sleep_before_playwright_driver_stop()
                pw.stop()
            except Exception as exc:  # noqa: BLE001
                logger.warning("Lỗi khi stop Playwright: {}", exc)
            _shared_pw_by_thread.pop(thread_id, None)
            _shared_pw_ref_by_thread.pop(thread_id, None)


def _context_or_page_already_closed_msg(exc: BaseException) -> bool:
    m = str(exc).lower()
    return "has been closed" in m or ("target page" in m and "closed" in m)


def is_playwright_target_closed_error(exc: BaseException) -> bool:
    """
    True khi page/context/browser đã đóng (crash, OOM, đóng cửa sổ).

    Không đồng nhất với người dùng bấm «Dừng» trên pool.
    """
    if type(exc).__name__ == "TargetClosedError":
        return True
    return _context_or_page_already_closed_msg(exc)


def _cleanup_firefox_profile_lock_files(user_data_dir: Path) -> None:
    """
    Xóa lock file mồ côi của Firefox profile trước khi launch.

    Trường hợp Firefox thoát đột ngột có thể để lại file lock khiến launch_persistent_context
    mở tiến trình rồi thoát ngay.
    """
    candidates = ("parent.lock", "lock", ".parentlock")
    for name in candidates:
        p = user_data_dir / name
        if not p.exists():
            continue
        try:
            p.unlink()
            logger.info("Đã xóa Firefox lock file mồ côi: {}", p)
        except OSError as exc:
            logger.debug("Không xóa được Firefox lock file {}: {}", p, exc)


def sync_close_persistent_context(
    context: BrowserContext | None,
    *,
    log_label: str = "",
    timeout_sec: float = 0.0,
    same_thread: bool = True,
    force_kill_firefox: bool | None = None,
) -> None:
    """
    Đóng ``BrowserContext`` persistent: đóng từng ``Page`` còn mở rồi ``context.close()``.

    Giảm race driver Node (``EPIPE``) khi Chromium còn phát sự kiện sau khi pipe đã đóng.
    ``timeout_sec`` > 0 và ``same_thread=False``: giới hạn thời gian đóng trên thread phụ.
    ``same_thread=True`` (mặc định): bắt buộc với Playwright Sync API.
    ``force_kill_firefox=False``: không ``terminate`` Firefox — giữ ``cookies.sqlite`` profile.
    """
    if context is None:
        return

    def _close_inner() -> None:
        _sync_close_persistent_context_impl(
            context,
            log_label=log_label,
            force_kill_firefox=force_kill_firefox,
        )

    if same_thread:
        _close_inner()
        return

    if timeout_sec and float(timeout_sec) > 0:
        import concurrent.futures

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
                fut = ex.submit(_close_inner)
                fut.result(timeout=max(3.0, float(timeout_sec)))
        except concurrent.futures.TimeoutError:
            logger.warning(
                "Timeout đóng BrowserContext sau {:.0f}s ({}) — bỏ qua để không chặn pool",
                float(timeout_sec),
                log_label or "(context)",
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Lỗi khi đóng BrowserContext có timeout ({}): {}", log_label, exc)
        return
    _close_inner()


def _sync_close_persistent_context_impl(
    context: BrowserContext,
    *,
    log_label: str = "",
    force_kill_firefox: bool | None = None,
) -> None:
    label = (log_label or "").strip() or "(context)"
    relay = getattr(context, "_toolfb_socks_relay", None)  # noqa: SLF001
    profile_lock = getattr(context, "_toolfb_profile_lock", None)
    profile_dir = getattr(context, "_toolfb_profile_dir", None)
    profile_account = getattr(context, "_toolfb_profile_account_id", None)
    try:
        for pg in list(context.pages):
            try:
                if not pg.is_closed():
                    pg.close()
            except Exception as exc:  # noqa: BLE001
                if _context_or_page_already_closed_msg(exc):
                    logger.debug("Page đã đóng ({}): {}", label, exc)
                else:
                    logger.debug("Bỏ qua đóng page trước context ({}): {}", label, exc)
    except Exception as exc:  # noqa: BLE001
        logger.debug("Không liệt kê page trước khi đóng context ({}): {}", label, exc)
    try:
        context.close()
    except Exception as exc:  # noqa: BLE001
        if _context_or_page_already_closed_msg(exc):
            logger.debug("BrowserContext đã đóng trước đó ({}): {}", label, exc)
        else:
            logger.warning("Lỗi khi đóng BrowserContext ({}): {}", label, exc)
    if relay is not None:
        try:
            relay.stop()
        except Exception as exc:  # noqa: BLE001
            logger.debug("Dừng SOCKS5 relay ({}): {}", label, exc)
        try:
            del context._toolfb_socks_relay  # type: ignore[attr-defined]
        except Exception:
            pass
    if profile_lock is not None and profile_dir is not None:
        try:
            release_profile_browser_slot(
                Path(profile_dir),
                str(profile_account or label),
                profile_lock,
            )
        except Exception as exc:  # noqa: BLE001
            logger.debug("Mở khóa profile ({}): {}", label, exc)
        for attr in ("_toolfb_profile_lock", "_toolfb_profile_dir", "_toolfb_profile_account_id"):
            try:
                delattr(context, attr)
            except Exception:
                pass
    if profile_dir is not None and sys.platform == "win32":
        if force_kill_firefox is None:
            force_kill = os.environ.get("FB_FORCE_KILL_FIREFOX_ON_CLOSE", "0").strip().lower() not in (
                "0",
                "false",
                "off",
                "no",
            )
        else:
            force_kill = bool(force_kill_firefox)
        if force_kill:
            try:
                from src.utils.win_browser_window import terminate_firefox_for_profile

                grace = max(500, int(float(os.environ.get("FB_FIREFOX_KILL_GRACE_MS", "2500"))))
                terminate_firefox_for_profile(Path(profile_dir), grace_ms=grace)
            except Exception as exc:  # noqa: BLE001
                logger.debug("Ep dong Firefox profile ({}): {}", label, exc)


def apply_viewport_from_env_to_page(page: Page, playwright: Playwright | None = None) -> None:
    """
    Ép khung trang khớp viewport (mặc định cố định compact desktop; mobile + preset tùy env).
    """
    # Chính sách runtime: automation luôn desktop viewport ổn định.
    mobile_mode = False
    mv = resolve_mobile_viewport(playwright, mobile_mode=mobile_mode)
    if getattr(mv, "use_mobile_facebook_shell", False):
        os.environ["TOOLFB_NAV_MOBILE_FB"] = "1"
    else:
        os.environ.pop("TOOLFB_NAV_MOBILE_FB", None)
    try:
        page.set_viewport_size({"width": mv.width, "height": mv.height})
        logger.info(
            "Đã set_viewport_size: {}x{} | device={}",
            mv.width,
            mv.height,
            mv.device_name,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không set_viewport_size ({}x{}): {}", mv.width, mv.height, exc)
    if _env_bool("FB_LOG_PAGE_USER_AGENT", False):
        try:
            nav_ua = page.evaluate("() => navigator.userAgent")
            logger.info("Trình duyệt (trang): navigator.userAgent={!r}", nav_ua)
        except Exception as exc:  # noqa: BLE001
            logger.debug("Không đọc được navigator.userAgent: {}", exc)


def _mobile_user_agent_default() -> str:
    # Safari iOS phổ biến; dùng để ép bề mặt mobile trên Facebook.
    return (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 "
        "Mobile/15E148 Safari/604.1"
    )


def account_use_proxy_enabled(acc: dict) -> bool:
    """
    Đọc cờ ``use_proxy`` trên bản ghi tài khoản (mặc định True nếu thiếu — tương thích JSON cũ).
    """
    v = acc.get("use_proxy", True)
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    s = str(v).strip().lower()
    if s in ("0", "false", "no", "off"):
        return False
    return True


def format_proxy_server_url(proxy: ProxyConfig) -> str:
    """
    Ghép chuỗi proxy dạng ``http://user:pass@host:port`` hoặc ``http://host:port`` nếu không có user.

    User/password được URL-encode để an toàn với ký tự đặc biệt.

    Args:
        proxy: Khối proxy trong accounts.json.

    Returns:
        Chuỗi server proxy dùng cho Playwright ``proxy.server``.
    """
    host = str(proxy.get("host", "")).strip()
    port = int(proxy.get("port", 0))
    user = (proxy.get("user") or "").strip()
    password = (proxy.get("pass") or "").strip()
    if user:
        u = quote(user, safe="")
        p = quote(password, safe="")
        return f"http://{u}:{p}@{host}:{port}"
    return f"http://{host}:{port}"


def playwright_proxy_settings(proxy: ProxyConfig) -> dict[str, Any]:
    """
    Cấu hình ``proxy`` cho ``launch_persistent_context`` (HTTP/HTTPS/SOCKS4/SOCKS5).

    Logic tập trung tại ``src.utils.proxy_check.build_playwright_proxy_settings``.
    """
    from src.utils.proxy_check import build_playwright_proxy_settings

    return build_playwright_proxy_settings(proxy)  # type: ignore[arg-type]


class BrowserFactory:
    """
    Factory khởi tạo ``BrowserContext`` persistent theo ``account_id``.

    Mặc định **mỗi** ``BrowserFactory`` = một ``sync_playwright()`` riêng (``owned``) — tách
    driver Node giữa các lần mở tài khoản. Bật chia sẻ theo thread: ``FB_PLAYWRIGHT_SHARED=1``.

    Mỗi tài khoản: ``launch_persistent_context`` + ``portable_path`` riêng; không hai account
    cùng profile; khóa ``acquire_profile_browser_slot``.

    Nếu inject ``playwright`` từ bên ngoài, factory không stop instance đó.
    """

    def __init__(
        self,
        accounts: Optional[AccountsDatabaseManager] = None,
        playwright: Optional[Playwright] = None,
        *,
        headless: bool = False,
        playwright_shared: bool | None = None,
    ) -> None:
        """
        Khởi tạo factory.

        Args:
            accounts: Bộ đọc accounts.json; mặc định dùng đường dẫn chuẩn trong dự án.
            playwright: Instance Playwright đã start; nếu None factory dùng shared hoặc tự start (xem env).
            headless: Mặc định chạy không/kèm cửa sổ (có thể ghi đè mỗi lần gọi ``get_browser_context``).
            playwright_shared: ``True`` = một driver Node/luồng worker (pool tương tác). ``None`` = đọc env.
        """
        self._accounts = accounts or AccountsDatabaseManager()
        self._playwright_closed = False
        self._pw_mode: Literal["injected", "shared", "owned"]
        use_shared = (
            bool(playwright_shared)
            if playwright_shared is not None
            else _env_bool("FB_PLAYWRIGHT_SHARED", False)
        )
        if playwright is not None:
            self._pw_mode = "injected"
            self._playwright = playwright
        elif use_shared:
            self._pw_mode = "shared"
            self._playwright = _acquire_shared_playwright()
        else:
            self._pw_mode = "owned"
            prepare_playwright_sync_thread(label="owned_factory")
            self._playwright = sync_playwright().start()
            logger.info(
                "Playwright riêng cho phiên tài khoản (owned) — thread_id={}",
                threading.get_ident(),
            )
        self._default_headless = headless
        logger.debug(
            "BrowserFactory khởi tạo (pw_mode={}, headless_mặc_định={})",
            self._pw_mode,
            self._default_headless,
        )

    @property
    def playwright(self) -> Playwright:
        """Instance Playwright đang dùng (để đồng bộ viewport sau khi mở trang)."""
        if self._playwright is None:
            raise RuntimeError("Playwright đã bị stop.")
        return self._playwright

    def close(self) -> None:
        """
        Giải phóng refcount shared / dừng Playwright nếu factory tự ``start`` (``FB_PLAYWRIGHT_SHARED=0``).

        Gọi sau khi đã ``close`` mọi ``BrowserContext`` trả về từ factory.
        """
        if self._playwright_closed:
            return
        self._playwright_closed = True
        if self._pw_mode == "injected":
            return
        if self._pw_mode == "shared":
            _release_shared_playwright()
            self._playwright = None
            return
        if self._playwright is not None:
            logger.info("Đang dừng Playwright (chế độ owned, FB_PLAYWRIGHT_SHARED=0).")
            try:
                _sleep_before_playwright_driver_stop()
                self._playwright.stop()
            except Exception as exc:  # noqa: BLE001
                logger.warning("Lỗi khi stop Playwright: {}", exc)
            self._playwright = None

    def _resolve_portable_dir(self, account: AccountRecord) -> Path:
        """
        Chuẩn hóa ``portable_path`` thành đường dẫn tuyệt đối (relative tính từ thư mục gốc dự án).

        Args:
            account: Bản ghi tài khoản.

        Returns:
            Thư mục user-data Chromium/Firefox/WebKit.
        """
        raw = str(account.get("portable_path") or account.get("profile_path") or "").strip()
        if not raw:
            raise ValueError(
                "Tài khoản chưa có portable_path/profile_path. "
                "Vui lòng cấu hình profile trong tab Tài khoản trước khi chạy."
            )
        path = Path(raw)
        if not path.is_absolute():
            path = _project_root() / path
        resolved = path.resolve()
        if resolved.is_dir():
            from src.utils.account_browser_profile import assert_profile_directory_owned_by

            assert_profile_directory_owned_by(
                str(account.get("registry_id") or account.get("id", "")).strip(),
                resolved,
                account=dict(account),
            )
            logger.info(
                "Profile portable | account={} | registry={} | path={}",
                account.get("display_account_id") or account.get("id"),
                account.get("registry_id") or account.get("id"),
                resolved,
            )
            return resolved
        allow_create = _env_bool("FB_AUTO_CREATE_PROFILE_DIR", False)
        if allow_create:
            resolved.mkdir(parents=True, exist_ok=True)
            logger.warning(
                "Tự tạo thư mục profile vì FB_AUTO_CREATE_PROFILE_DIR=1: {}",
                resolved,
            )
            return resolved
        raise ValueError(
            f"Thư mục profile không tồn tại: {resolved}. "
            "Để tránh mở nhầm profile mới, app không tự tạo thư mục. "
            "Hãy kiểm tra portable_path/profile_path hoặc bật FB_AUTO_CREATE_PROFILE_DIR=1 nếu thực sự muốn tạo mới."
        )

    def _select_browser_type(self, name: str):
        """
        Chọn đối tượng ``BrowserType`` trên Playwright theo chuỗi cấu hình.

        Args:
            name: Ví dụ ``chromium``, ``firefox``, ``webkit`` (không phân biệt hoa thường).

        Returns:
            sync_api.BrowserType tương ứng.

        Raises:
            ValueError: Loại trình duyệt không được hỗ trợ.
        """
        key = (name or "chromium").strip().lower()
        if key in ("chromium", "chrome"):
            return self._playwright.chromium
        if key == "firefox":
            return self._playwright.firefox
        if key == "webkit":
            return self._playwright.webkit
        raise ValueError(f"browser_type không hỗ trợ: {name}")

    def _launch_args_for(
        self,
        browser_key: str,
        *,
        viewport_width: int | None = None,
        viewport_height: int | None = None,
        window_x: int | None = None,
        window_y: int | None = None,
        fit_outer_window: bool = False,
    ) -> list[str]:
        """
        Trả về ``args`` khởi chạy theo loại trình duyệt (chống phát hiện bot cho Chromium).

        Args:
            browser_key: ``chromium`` / ``firefox`` / ``webkit``.

        Returns:
            Danh sách tham số dòng lệnh.
        """
        if browser_key in ("chromium", "chrome"):
            # Mặc định tối giản (gần bản gốc): tránh chồng cờ “tối ưu” dễ làm Facebook/Chromium cảm giác chậm hơn.
            # Thêm cờ tùy chọn: FB_CHROMIUM_EXTRA_LAUNCH_ARGS="--foo --bar" (tách bằng khoảng trắng).
            args = [
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
            ]
            use_bundled = (
                getattr(sys, "frozen", False)
                or _env_bool("TOOLFB_ENFORCE_BUNDLED_BROWSER", False)
                or str(os.environ.get("FB_PLAYWRIGHT_CHROMIUM_CHANNEL", "")).strip().lower()
                in {"0", "false", "off", "no", "bundled", "bundle", "playwright", "ms-playwright", "chromium"}
            )
            if use_bundled:
                for extra in ("--disable-component-update",):
                    _append_unique_arg(args, extra)
            raw_extra = os.environ.get("FB_CHROMIUM_EXTRA_LAUNCH_ARGS", "").strip()
            if raw_extra:
                args.extend([p for p in raw_extra.split() if p])
            if viewport_width and viewport_height:
                args.append(f"--window-size={viewport_width},{viewport_height}")
            if window_x is not None and window_y is not None:
                args.append(f"--window-position={int(window_x)},{int(window_y)}")
            return args
        if browser_key == "firefox":
            args: list[str] = []
            # Lưới đa cửa sổ: bật -width/-height; mặc định tắt để tránh lệch layout khi không chia lưới.
            if (
                fit_outer_window or _env_bool("FB_FIREFOX_OUTER_WINDOW_FIT", False)
            ) and viewport_width and viewport_height:
                args.extend(["-width", str(viewport_width), "-height", str(viewport_height)])
            if window_x is not None and window_y is not None:
                args.extend(["-left", str(int(window_x)), "-top", str(int(window_y))])
            return args
        return []

    def get_browser_context(
        self,
        account_id: str,
        *,
        headless: Optional[bool] = None,
    ) -> BrowserContext:
        """
        Đọc account, áp dụng proxy, mở ``launch_persistent_context`` và gắn playwright-stealth.

        Args:
            account_id: Trường ``id`` trong accounts.json.
            headless: Ghi đè chế độ headless; None dùng mặc định của factory.

        Returns:
            ``BrowserContext`` persistent (caller chịu trách nhiệm ``context.close()``).

        Raises:
            ValueError: Không tìm thấy account hoặc cấu hình không hợp lệ.
        """
        if self._playwright is None:
            raise RuntimeError("Playwright đã bị stop; tạo BrowserFactory mới hoặc không gọi close() quá sớm.")

        acc = self._accounts.get_by_id(account_id)
        if acc is None:
            logger.error("Không tìm thấy account_id={}", account_id)
            raise ValueError(f"Không tìm thấy tài khoản: {account_id}")
        return self.launch_persistent_context_from_account_dict(dict(acc), headless=headless)

    def launch_persistent_context_from_account_dict(
        self,
        acc: dict[str, Any],
        *,
        headless: Optional[bool] = None,
        grid_viewport: tuple[int, int] | None = None,
        window_position: tuple[int, int] | None = None,
        disable_notifications: bool = False,
        launch_slot_timeout_sec: float | None = None,
        skip_geo_lookup: bool = False,
        force_desktop_facebook: bool = False,
    ) -> BrowserContext:
        """
        Mở persistent context từ dict (cùng khóa như ``accounts.json``), không cần bản ghi đã lưu DB.

        Dùng cho form «Thêm mới» — đăng nhập Facebook rồi ``storage_state`` ra ``cookie_path``.

        Args:
            grid_viewport: ``(width, height)`` từ Grid Layout Manager; ghi đè viewport mặc định.
            window_position: ``(x, y)`` góc trái cửa sổ (Chromium ``--window-position``; Firefox ``-left``/``-top`` + Win32).
            disable_notifications: Tắt thông báo trình duyệt (Firefox pref / Chromium args).
            launch_slot_timeout_sec: Chờ slot launch tối đa (giây); ``None`` = chờ vô hạn.
            skip_geo_lookup: Bỏ tra ip-api qua proxy (mở tay nhanh hơn).
            force_desktop_facebook: Luôn ``www.facebook.com`` + viewport desktop (tab Tương tác sau đăng nhập).
        """
        if self._playwright is None:
            raise RuntimeError("Playwright đã bị stop; tạo BrowserFactory mới hoặc không gọi close() quá sớm.")

        account_id = str(acc.get("id", "")).strip() or "(preview)"
        _raw_bt = acc.get("browser_type", "firefox")
        browser_key = str(_raw_bt if _raw_bt is not None else "firefox").strip().lower() or "firefox"
        user_data_dir = self._resolve_portable_dir(acc)  # type: ignore[arg-type]
        proxy_cfg = acc.get("proxy")
        if not isinstance(proxy_cfg, dict):
            raise ValueError("Trường proxy của tài khoản không hợp lệ.")
        use_px = account_use_proxy_enabled(acc)
        apply_proxy = use_px and proxy_host_port_configured(proxy_cfg)  # type: ignore[arg-type]

        logger.info(
            "Khởi tạo persistent context (account={}, browser={}, profile={})",
            account_id,
            browser_key,
            user_data_dir,
        )
        socks_relay = None
        if apply_proxy:
            from src.utils.socks5_http_relay import Socks5HttpRelay, socks_proxy_needs_http_relay

            if socks_proxy_needs_http_relay(proxy_cfg) and not _env_bool("TOOLFB_DISABLE_SOCKS_RELAY", False):
                socks_relay = Socks5HttpRelay.from_proxy_config(proxy_cfg)  # type: ignore[arg-type]
                socks_relay.start()
                pw_proxy = {"server": socks_relay.local_url}
                logger.info(
                    "Proxy Playwright | SOCKS5 có auth → relay HTTP local {} (upstream socks5://{}:{})",
                    socks_relay.local_url,
                    socks_relay._upstream_host,
                    socks_relay._upstream_port,
                )
            else:
                pw_proxy = playwright_proxy_settings(proxy_cfg)  # type: ignore[arg-type]
                logger.info(
                    "Proxy Playwright | server={} | có_user={}",
                    pw_proxy["server"],
                    bool(pw_proxy.get("username")),
                )
            logger.debug("Proxy URL tương đương (log ẩn pass): {}", _redact_proxy_url(format_proxy_server_url(proxy_cfg)))
        elif use_px:
            logger.warning(
                "use_proxy bật nhưng thiếu host/port hợp lệ — chạy không proxy (account={}).",
                account_id,
            )
        else:
            logger.info("Không dùng proxy (use_proxy=false) account={}.", account_id)

        use_headless = self._default_headless if headless is None else headless
        browser_type = self._select_browser_type(browser_key)
        manual_open = launch_slot_timeout_sec is not None
        use_desktop_fb = manual_open or force_desktop_facebook
        # Chính sách runtime: automation luôn desktop viewport ổn định.
        mobile_mode = False
        mv = resolve_mobile_viewport(self._playwright, mobile_mode=mobile_mode)
        if grid_viewport and len(grid_viewport) >= 2:
            # Lưới đa cửa sổ: viewport = kích thước ô (tránh cửa sổ 1280px chồng lên nhau).
            vp_w, vp_h = max(320, int(grid_viewport[0])), max(400, int(grid_viewport[1]))
            if use_desktop_fb:
                logger.info(
                    "[Browser] Lưới {}×{} + www/desktop (account={})",
                    vp_w,
                    vp_h,
                    account_id,
                )
        elif use_desktop_fb:
            vp_w, vp_h = mv.width, mv.height
        else:
            vp_w, vp_h = mv.width, mv.height
        win_x = int(window_position[0]) if window_position and len(window_position) >= 1 else None
        win_y = int(window_position[1]) if window_position and len(window_position) >= 2 else None
        safe_w = max(320, _env_int("FB_DESKTOP_SAFE_MIN_WIDTH", 900))
        auto_narrow = _env_bool("FB_AUTO_MOBILE_WEB_WHEN_NARROW", True)
        if force_desktop_facebook:
            shell = False
        elif not mobile_mode:
            shell = bool(auto_narrow and vp_w < safe_w)
        else:
            shell = bool(getattr(mv, "use_mobile_facebook_shell", False))
        if shell:
            os.environ["TOOLFB_NAV_MOBILE_FB"] = "1"
        else:
            os.environ.pop("TOOLFB_NAV_MOBILE_FB", None)
        effective_mobile = mobile_mode or shell
        ua_explicit = (
            os.environ.get("PLAYWRIGHT_USER_AGENT", "").strip()
            or os.environ.get("FB_MOBILE_UA", "").strip()
        )
        if ua_explicit:
            ua = ua_explicit
        elif mv.user_agent:
            ua = mv.user_agent
        elif effective_mobile:
            ua = _mobile_user_agent_default()
        else:
            ua = default_desktop_firefox_user_agent()

        proxy_cfg_for_fp = proxy_cfg if apply_proxy else None
        if apply_proxy and not skip_geo_lookup:
            geo_fp = resolve_proxy_geo(acc, proxy_cfg_for_fp)
        else:
            geo_fp = None
            if apply_proxy and skip_geo_lookup:
                logger.info(
                    "[Browser] Bỏ tra geo ip-api (mở tay) — locale/timezone mặc định account={}",
                    account_id,
                )
        locale_id = resolve_browser_locale(acc, proxy=proxy_cfg_for_fp, geo=geo_fp)
        timezone_id = resolve_browser_timezone(acc, proxy=proxy_cfg_for_fp, geo=geo_fp)
        place_window = bool(not use_headless and win_x is not None and win_y is not None)
        fit_outer_from_grid = bool(grid_viewport and not use_headless)
        args = self._launch_args_for(
            browser_key,
            viewport_width=vp_w if not use_headless else None,
            viewport_height=vp_h if not use_headless else None,
            window_x=win_x if not use_headless else None,
            window_y=win_y if not use_headless else None,
            fit_outer_window=fit_outer_from_grid,
        )
        if disable_notifications and browser_key in ("chromium", "chrome"):
            _append_unique_arg(args, "--disable-notifications")
        if browser_key in ("chromium", "chrome"):
            # Chỉ bật khi người dùng chủ động yêu cầu; một số máy có thể chậm/hang khi áp cờ mạng quá mạnh.
            if _env_bool("FB_CHROMIUM_FAST_NETWORK", False):
                for extra in (
                    "--disable-background-networking",
                    "--disable-component-update",
                    "--disable-domain-reliability",
                    "--disable-features=OptimizationHints,MediaRouter",
                ):
                    _append_unique_arg(args, extra)
            # Chỉ ép direct proxy khi bật cờ rõ ràng để tránh ảnh hưởng mạng đặc thù.
            if _env_bool("FB_FORCE_DIRECT_NO_PROXY", False) and not apply_proxy and not _env_bool("FB_KEEP_SYSTEM_PROXY", False):
                for extra in ("--no-proxy-server", "--proxy-bypass-list=*"):
                    _append_unique_arg(args, extra)

        launch_kwargs = dict(
            user_data_dir=str(user_data_dir),
            headless=use_headless,
            args=args,
            locale=locale_id,
            timezone_id=timezone_id,
            viewport={"width": vp_w, "height": vp_h},
            # Cùng kích thước screen để window.screen khớp viewport, tránh trang lỗi / layout tính sai chiều ngang.
            screen={"width": vp_w, "height": vp_h},
        )
        if geo_fp:
            logger.info(
                "Browser fingerprint | locale={} | timezone={} | geo_ip={} | country={}",
                locale_id,
                timezone_id,
                geo_fp.get("ip", ""),
                geo_fp.get("country_code", ""),
            )
        if browser_key in ("chromium", "chrome"):
            raw_to = os.environ.get("FB_CHROMIUM_LAUNCH_TIMEOUT_MS", "").strip()
            if raw_to:
                try:
                    launch_kwargs["timeout"] = max(1, int(raw_to))
                except ValueError:
                    logger.warning("FB_CHROMIUM_LAUNCH_TIMEOUT_MS={!r} không hợp lệ — bỏ qua.", raw_to)
        elif browser_key == "firefox":
            raw_ff = os.environ.get("FB_FIREFOX_LAUNCH_TIMEOUT_MS", "90000").strip()
            try:
                launch_kwargs["timeout"] = max(15_000, int(raw_ff))
            except ValueError:
                launch_kwargs["timeout"] = 90_000
        if ua:
            launch_kwargs["user_agent"] = ua
        if effective_mobile and browser_key in ("chromium", "chrome", "webkit"):
            launch_kwargs["is_mobile"] = True
            launch_kwargs["has_touch"] = True
            launch_kwargs["device_scale_factor"] = float(mv.device_scale_factor)
        elif effective_mobile and browser_key == "firefox":
            # Firefox: không có is_mobile; vẫn cần DPR + touch để Juggler setDefaultViewport khớp (giảm dải trắng).
            launch_kwargs["device_scale_factor"] = float(mv.device_scale_factor)
            launch_kwargs["has_touch"] = True
            launch_kwargs["firefox_user_prefs"] = merge_firefox_user_prefs(
                {"privacy.resistFingerprinting": False}
            )
            logger.warning(
                "Chế độ mobile shell (FB_MOBILE hoặc viewport hẹp) + firefox: không có is_mobile của Chromium. "
                "Nếu layout lệch, thử browser_type=chromium."
            )
        if browser_key == "firefox":
            ff = merge_firefox_user_prefs(dict(launch_kwargs.get("firefox_user_prefs") or {}))
            if disable_notifications:
                ff["dom.webnotifications.enabled"] = False
            if not apply_proxy:
                # Ép kết nối trực tiếp: profile portable có thể còn PAC/manual proxy từ máy cũ → NS_ERROR_PROXY_*.
                ff.update(
                    {
                        "network.proxy.type": 0,
                        "network.proxy.http": "",
                        "network.proxy.http_port": 0,
                        "network.proxy.ssl": "",
                        "network.proxy.ssl_port": 0,
                        "network.proxy.socks": "",
                        "network.proxy.socks_port": 0,
                        "network.proxy.share_proxy_settings": True,
                        "network.proxy.autoconfig_url": "",
                    }
                )
            launch_kwargs["firefox_user_prefs"] = ff
        if apply_proxy:
            if socks_relay is not None:
                launch_kwargs["proxy"] = {"server": socks_relay.local_url}
            else:
                launch_kwargs["proxy"] = playwright_proxy_settings(proxy_cfg)  # type: ignore[arg-type]
        elif not _env_bool("FB_KEEP_SYSTEM_PROXY", False):
            clean = {k: v for k, v in os.environ.items()}
            for k in (
                "HTTP_PROXY",
                "HTTPS_PROXY",
                "ALL_PROXY",
                "http_proxy",
                "https_proxy",
                "all_proxy",
            ):
                clean.pop(k, None)
            launch_kwargs["env"] = clean

        # Hiển thị rõ User Agent để người dùng theo dõi trực quan trong GUI log.
        logger.info(
            "Browser account={} | browser_type={} | profile={} | mobile_mode={} | m_fb_shell={} | mobile_device={} | viewport={}x{} | dpr={} | user_agent={}",
            account_id,
            browser_key,
            user_data_dir,
            mobile_mode,
            shell,
            mv.device_name,
            vp_w,
            vp_h,
            mv.device_scale_factor,
            ua or "(default browser UA)",
        )

        try:
            from src.utils.playwright_browser_lock import assert_browsers_ready_for_launch

            assert_browsers_ready_for_launch(
                project_root=_project_root(),
                browser_key=browser_key,
            )
        except RuntimeError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.debug("assert_browsers_ready_for_launch: {}", exc)

        exe = str(acc.get("browser_exe_path", "")).strip()
        has_exe = False
        if exe:
            pexe = Path(exe)
            if pexe.is_file():
                launch_kwargs["executable_path"] = str(pexe.resolve())
                has_exe = True
            else:
                logger.warning("browser_exe_path không tồn tại: {} (account={})", exe, account_id)
        ch = _playwright_chromium_channel(browser_key=browser_key, has_executable_path=has_exe)
        if ch:
            launch_kwargs["channel"] = ch
            logger.info(
                "Chromium: channel={} (trình duyệt hệ thống — thường ổn định hơn Chromium bundle Playwright).",
                ch,
            )

        if browser_key == "firefox":
            _cleanup_firefox_profile_lock_files(user_data_dir)

        profile_lock = acquire_profile_browser_slot(
            user_data_dir,
            account_id,
            timeout_sec=launch_slot_timeout_sec if launch_slot_timeout_sec is not None else None,
        )
        acquired_launch_sem = False
        try:
            if manual_open:
                logger.info(
                    "[Browser] Mở trình duyệt thủ công account={} — không chờ semaphore pool.",
                    account_id,
                )
            else:
                _acquire_browser_launch_slot(
                    account_id=account_id,
                    timeout_sec=launch_slot_timeout_sec,
                )
                acquired_launch_sem = True
            try:
                try:
                    context = browser_type.launch_persistent_context(**launch_kwargs)
                except Exception as exc:
                    msg = str(exc).lower()
                    if "executable doesn't exist" in msg or "executable does not exist" in msg:
                        from src.utils.playwright_browser_lock import browser_executable_missing_message

                        raise RuntimeError(
                            browser_executable_missing_message(
                                browser_key=browser_key,
                                project_root=_project_root(),
                            )
                        ) from exc
                    # Firefox thỉnh thoảng launch xong rồi thoát ngay (exitCode=0) do lock/profile state.
                    if browser_key == "firefox" and "process did exit: exitcode=0" in msg:
                        logger.warning(
                            "Firefox launch thoát sớm (account={}) — thử dọn lock profile và launch lại 1 lần.",
                            account_id,
                        )
                        _cleanup_firefox_profile_lock_files(user_data_dir)
                        time.sleep(0.25)
                        try:
                            context = browser_type.launch_persistent_context(**launch_kwargs)
                        except Exception as exc2:
                            msg2 = str(exc2).lower()
                            if "process did exit: exitcode=0" not in msg2:
                                raise
                            allow_cross_engine_fallback = _env_bool("FB_ALLOW_CROSS_ENGINE_FALLBACK", False)
                            if not allow_cross_engine_fallback:
                                raise RuntimeError(
                                    "Firefox profile thoát sớm (exitCode=0) sau 2 lần thử. "
                                    "Để tránh mở sai phiên đăng nhập, app KHÔNG tự đổi sang Chromium. "
                                    "Bạn có thể bật FB_ALLOW_CROSS_ENGINE_FALLBACK=1 nếu muốn fallback tạm."
                                ) from exc2
                            logger.warning(
                                "Firefox vẫn thoát sớm sau retry (account={}) — fallback sang Chromium vì FB_ALLOW_CROSS_ENGINE_FALLBACK=1.",
                                account_id,
                            )
                            fallback_kwargs = dict(launch_kwargs)
                            fallback_kwargs.pop("firefox_user_prefs", None)
                            fallback_kwargs.pop("executable_path", None)
                            fallback_kwargs["args"] = self._launch_args_for(
                                "chromium",
                                viewport_width=vp_w if not use_headless else None,
                                viewport_height=vp_h if not use_headless else None,
                                window_x=win_x if not use_headless else None,
                                window_y=win_y if not use_headless else None,
                                fit_outer_window=bool(grid_viewport and not use_headless),
                            )
                            ch_fb = _playwright_chromium_channel(browser_key="chromium", has_executable_path=False)
                            if ch_fb:
                                fallback_kwargs["channel"] = ch_fb
                            context = self._playwright.chromium.launch_persistent_context(**fallback_kwargs)
                    else:
                        raise
            finally:
                if acquired_launch_sem:
                    _browser_launch_semaphore().release()
        except Exception:
            release_profile_browser_slot(user_data_dir, account_id, profile_lock)
            raise
        stealth = _stealth_for_project(use_mobile_fingerprint=effective_mobile)
        if stealth is None:
            if _STEALTH_IMPORT_ERROR is not None:
                logger.warning(
                    "Không nạp được playwright-stealth, tiếp tục không stealth: {}",
                    _STEALTH_IMPORT_ERROR,
                )
        else:
            try:
                stealth.apply_stealth_sync(context)
                logger.info("Đã áp dụng playwright-stealth lên BrowserContext (account={}).", account_id)
            except Exception as stealth_exc:  # noqa: BLE001
                logger.warning("Lỗi apply playwright-stealth, tiếp tục không stealth: {}", stealth_exc)
        apply_fingerprint_init_script(context)
        try:
            from src.services.facebook_recaptcha import install_recaptcha_network_interception

            install_recaptcha_network_interception(context)
        except Exception as cap_exc:  # noqa: BLE001
            logger.warning("[FB reCAPTCHA] Không cài network interception: {}", cap_exc)
        if socks_relay is not None:
            context._toolfb_socks_relay = socks_relay  # type: ignore[attr-defined]
        context._toolfb_profile_lock = profile_lock  # type: ignore[attr-defined]
        context._toolfb_profile_dir = str(user_data_dir)  # type: ignore[attr-defined]
        context._toolfb_profile_account_id = account_id  # type: ignore[attr-defined]
        if place_window and browser_key == "firefox":
            try:
                from src.utils.win_browser_window import (
                    firefox_outer_window_size,
                    place_firefox_window_for_profile,
                )

                outer_w, outer_h = firefox_outer_window_size(vp_w, vp_h)
                place_firefox_window_for_profile(
                    user_data_dir,
                    x=win_x,
                    y=win_y,
                    width=outer_w,
                    height=outer_h,
                )
            except Exception as place_exc:  # noqa: BLE001
                logger.warning("Không đặt được vị trí cửa sổ Firefox (grid): {}", place_exc)

        logger.info(
            "Đã mở Firefox/{} persistent | account={} | profile={} | pw_mode={} | proxy={} | grid_pos={}",
            browser_key,
            account_id,
            user_data_dir,
            self._pw_mode,
            bool(apply_proxy),
            f"{win_x},{win_y}" if win_x is not None and win_y is not None else "-",
        )
        return context


def _redact_proxy_url(url: str) -> str:
    """
    Rút gọn URL proxy để log (ẩn user/password).

    Args:
        url: URL đầy đủ ``http://user:pass@host:port``.

    Returns:
        Chuỗi an toàn khi ghi log.
    """
    if "@" not in url:
        return url
    try:
        scheme, rest = url.split("://", 1)
        _creds, hostpart = rest.rsplit("@", 1)
        return f"{scheme}://***:***@{hostpart}"
    except ValueError:
        return "http://***"
