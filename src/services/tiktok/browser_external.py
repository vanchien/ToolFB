from __future__ import annotations

import os
import subprocess
import threading
import time
from dataclasses import dataclass
from shutil import which
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from loguru import logger
from playwright.sync_api import sync_playwright

from src.services.tiktok.layout import resolve_tiktok_profile_dir
from src.utils.browser_exe_discover import find_browser_exe_in_directory
from src.utils.paths import project_root


@dataclass
class _ManualSession:
    stop_event: threading.Event
    thread: threading.Thread | None = None
    proc: subprocess.Popen[Any] | None = None


_MANUAL_SESSIONS: dict[str, _ManualSession] = {}
_MANUAL_SESSIONS_LOCK = threading.Lock()


def close_tiktok_manual_profile_session(account_id: str) -> bool:
    """Đóng phiên browser manual login đang mở cho account TikTok."""
    aid = str(account_id or "").strip()
    if not aid:
        return False
    with _MANUAL_SESSIONS_LOCK:
        ses = _MANUAL_SESSIONS.pop(aid, None)
    if ses is None:
        return False
    ses.stop_event.set()
    if ses.proc is not None:
        try:
            if ses.proc.poll() is None:
                ses.proc.terminate()
                try:
                    ses.proc.wait(timeout=2.0)
                except Exception:
                    ses.proc.kill()
        except Exception:
            pass
    if ses.thread is not None and ses.thread.is_alive():
        try:
            ses.thread.join(timeout=2.0)
        except Exception:
            pass
    return True


def _set_tiktok_manual_session(account_id: str, ses: _ManualSession) -> None:
    with _MANUAL_SESSIONS_LOCK:
        _MANUAL_SESSIONS[account_id] = ses


def _clear_tiktok_manual_session(account_id: str, ses: _ManualSession) -> None:
    with _MANUAL_SESSIONS_LOCK:
        cur = _MANUAL_SESSIONS.get(account_id)
        if cur is ses:
            _MANUAL_SESSIONS.pop(account_id, None)


def open_chromium_like_profile(
    *,
    browser_exe: Path,
    profile_dir: Path,
    start_url: str | None = None,
    extra_args: list[str] | None = None,
) -> subprocess.Popen[Any]:
    """
    Mở Chrome/Chromium/Edge kiểu portable với ``--user-data-dir`` (đăng nhập thủ công, không giữ Playwright).
    """
    if not browser_exe.is_file():
        raise FileNotFoundError(f"Không tìm thấy browser: {browser_exe}")
    profile_dir.mkdir(parents=True, exist_ok=True)
    cmd = [str(browser_exe.resolve()), f"--user-data-dir={str(profile_dir.resolve())}"]
    if extra_args:
        cmd.extend([str(x) for x in extra_args if str(x).strip()])
    if str(start_url or "").strip():
        cmd.append(str(start_url).strip())
    return subprocess.Popen(cmd, shell=False)


def _normalized_proxy(account: dict[str, Any]) -> tuple[str, str, int] | None:
    px = account.get("proxy")
    if not isinstance(px, dict):
        return None
    if not bool(px.get("enabled")):
        return None
    raw = str(px.get("server", "")).strip()
    if not raw:
        return None
    raw_norm = raw if "://" in raw else f"http://{raw}"
    parsed = urlparse(raw_norm)
    host = (parsed.hostname or "").strip()
    port = int(parsed.port or 0)
    if not host or not (1 <= port <= 65535):
        return None
    scheme = (parsed.scheme or "http").strip().lower()
    if scheme not in {"http", "https", "socks5"}:
        scheme = "http"
    return (scheme, host, port)


def _chromium_proxy_args(account: dict[str, Any]) -> list[str]:
    norm = _normalized_proxy(account)
    if not norm:
        return []
    scheme, host, port = norm
    return [f"--proxy-server={scheme}://{host}:{port}"]


def _playwright_proxy(account: dict[str, Any]) -> dict[str, Any] | None:
    norm = _normalized_proxy(account)
    if not norm:
        return None
    scheme, host, port = norm
    out: dict[str, Any] = {"server": f"{scheme}://{host}:{port}"}
    user = str((account.get("proxy") or {}).get("username", "")).strip() if isinstance(account.get("proxy"), dict) else ""
    pwd = str((account.get("proxy") or {}).get("password", "")).strip() if isinstance(account.get("proxy"), dict) else ""
    if user:
        out["username"] = user
        out["password"] = pwd
    return out


def _write_firefox_proxy_user_js(profile_dir: Path, account: dict[str, Any]) -> None:
    """
    Áp proxy cho Firefox profile bằng ``user.js`` trước khi mở browser.
    Lưu ý: user/pass proxy có thể vẫn bị hỏi lại tùy server/chính sách Firefox.
    """
    norm = _normalized_proxy(account)
    if not norm:
        return
    scheme, host, port = norm
    lines = [
        'user_pref("network.proxy.type", 1);',
        'user_pref("network.proxy.no_proxies_on", "");',
        'user_pref("network.proxy.share_proxy_settings", true);',
        'user_pref("signon.autologin.proxy", true);',
    ]
    if scheme.startswith("socks"):
        lines.extend(
            [
                f'user_pref("network.proxy.socks", "{host}");',
                f'user_pref("network.proxy.socks_port", {port});',
                'user_pref("network.proxy.socks_version", 5);',
            ]
        )
    else:
        lines.extend(
            [
                f'user_pref("network.proxy.http", "{host}");',
                f'user_pref("network.proxy.http_port", {port});',
                f'user_pref("network.proxy.ssl", "{host}");',
                f'user_pref("network.proxy.ssl_port", {port});',
            ]
        )
    profile_dir.mkdir(parents=True, exist_ok=True)
    (profile_dir / "user.js").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _first_existing_file(candidates: list[Path]) -> Path | None:
    for p in candidates:
        try:
            if p.is_file():
                return p
        except Exception:
            continue
    return None


def _detect_chromium_exe() -> Path | None:
    # 1) PATH
    for cand in ("msedge.exe", "chrome.exe", "chromium.exe", "brave.exe", "vivaldi.exe"):
        hit = which(cand)
        if hit:
            p = Path(hit)
            if p.is_file():
                return p

    # 2) Common Windows install paths
    pf = os.environ.get("ProgramFiles", "").strip()
    pfx86 = os.environ.get("ProgramFiles(x86)", "").strip()
    local = os.environ.get("LOCALAPPDATA", "").strip()
    roots = [Path(x) for x in (pf, pfx86, local) if x]
    cands: list[Path] = []
    for r in roots:
        cands.extend(
            [
                r / "Google" / "Chrome" / "Application" / "chrome.exe",
                r / "Microsoft" / "Edge" / "Application" / "msedge.exe",
                r / "Chromium" / "Application" / "chrome.exe",
                r / "BraveSoftware" / "Brave-Browser" / "Application" / "brave.exe",
                r / "Vivaldi" / "Application" / "vivaldi.exe",
            ]
        )
    return _first_existing_file(cands)


def _detect_firefox_exe() -> Path | None:
    # 1) PATH
    for cand in ("firefox.exe", "firefox"):
        hit = which(cand)
        if hit:
            p = Path(hit)
            if p.is_file():
                return p

    # 2) Common Windows install paths
    pf = os.environ.get("ProgramFiles", "").strip()
    pfx86 = os.environ.get("ProgramFiles(x86)", "").strip()
    local = os.environ.get("LOCALAPPDATA", "").strip()
    roots = [Path(x) for x in (pf, pfx86, local) if x]
    cands: list[Path] = []
    for r in roots:
        cands.extend(
            [
                r / "Mozilla Firefox" / "firefox.exe",
                r / "Firefox Developer Edition" / "firefox.exe",
                r / "Waterfox" / "waterfox.exe",
            ]
        )
    hit = _first_existing_file(cands)
    if hit is not None:
        return hit
    return _detect_firefox_exe_from_playwright_cache()


def _detect_firefox_exe_from_playwright_cache() -> Path | None:
    """
    Dò Firefox trong runtime Playwright (cache hệ thống + bundle _internal của app).
    """
    cands: list[Path] = []
    local = os.environ.get("LOCALAPPDATA", "").strip()
    if local:
        ms_root = Path(local) / "ms-playwright"
        if ms_root.is_dir():
            for d in sorted(ms_root.glob("firefox-*"), reverse=True):
                cands.append(d / "firefox" / "firefox.exe")
    try:
        pr = project_root()
        bundled_roots = [
            pr / "_internal" / "ms-playwright",
            pr / "dist" / "ToolFB_GUI" / "_internal" / "ms-playwright",
        ]
        for br in bundled_roots:
            if not br.is_dir():
                continue
            for d in sorted(br.glob("firefox-*"), reverse=True):
                cands.append(d / "firefox" / "firefox.exe")
    except Exception:
        pass
    return _first_existing_file(cands)


def _detect_firefox_exe_from_profile(profile_dir: Path) -> Path | None:
    """
    Bổ sung dò Firefox portable gần profile để giảm phụ thuộc cài đặt Program Files.
    """
    roots: list[Path] = []
    try:
        rp = profile_dir.resolve()
        roots.extend([rp, rp.parent, rp.parent.parent])
    except Exception:
        pass
    try:
        roots.append(project_root() / "tools")
    except Exception:
        pass
    seen: set[str] = set()
    for root in roots:
        try:
            key = str(root.resolve()).lower()
        except Exception:
            key = str(root).lower()
        if key in seen:
            continue
        seen.add(key)
        found = find_browser_exe_in_directory(root)
        if found:
            p = Path(found)
            if p.is_file() and p.name.lower() in {"firefox.exe", "waterfox.exe"}:
                return p
    return None


def open_tiktok_profile_for_manual_login(account: dict[str, Any], *, show_browser: bool = True) -> None:
    aid = str(account.get("id", "")).strip() or "default"
    close_tiktok_manual_profile_session(aid)
    exe = Path(str(account.get("browser_exe_path", "")).strip())
    # Luôn dùng profile do ToolFB quản lý trong data/tiktok/profiles/<account_id>.
    prof = resolve_tiktok_profile_dir(account)
    bt = str(account.get("browser_type", "chrome") or "chrome").strip().lower()
    if bt == "firefox":
        prof.mkdir(parents=True, exist_ok=True)
        firefox_exe = exe if exe.is_file() else (_detect_firefox_exe_from_profile(prof) or _detect_firefox_exe())
        if firefox_exe is None:
            raise FileNotFoundError(
                "Không tìm thấy Firefox executable tự động. "
                "Hãy cài Firefox (Program Files), đặt Firefox portable gần profile, "
                "hoặc điền browser_exe_path trỏ tới firefox.exe."
            )
        if not firefox_exe.is_file():
            raise FileNotFoundError(
                "Không tìm thấy Firefox executable. Cài Firefox hoặc cấu hình browser_exe_path trỏ tới firefox.exe."
            )
        px = _playwright_proxy(account)
        # Proxy có user/pass: mở bằng Playwright để auth proxy tự động, tránh popup nhập tay.
        if isinstance(px, dict) and str(px.get("username", "")).strip():
            stop_event = threading.Event()

            def _run_firefox_with_playwright() -> None:
                ses = _ManualSession(stop_event=stop_event, thread=threading.current_thread())
                _set_tiktok_manual_session(aid, ses)
                try:
                    with sync_playwright() as p:
                        kwargs: dict[str, Any] = {
                            "user_data_dir": str(prof.resolve()),
                            "headless": (not bool(show_browser)),
                            "proxy": px,
                            "accept_downloads": True,
                            "viewport": {"width": 1280, "height": 900},
                        }
                        if firefox_exe.is_file():
                            kwargs["executable_path"] = str(firefox_exe.resolve())
                        ctx = p.firefox.launch_persistent_context(**kwargs)
                        if not ctx.pages:
                            ctx.new_page()
                        try:
                            while not stop_event.is_set():
                                if bool(show_browser) and len(ctx.pages) == 0:
                                    break
                                time.sleep(0.8)
                        finally:
                            try:
                                ctx.close()
                            except Exception:
                                pass
                except Exception as exc:  # noqa: BLE001
                    logger.warning("TikTok manual login (firefox+proxy auth) fallback thất bại: {}", exc)
                finally:
                    _clear_tiktok_manual_session(aid, ses)

            th = threading.Thread(target=_run_firefox_with_playwright, name="tt_manual_login_firefox_proxy", daemon=True)
            th.start()
            return
        _write_firefox_proxy_user_js(prof, account)
        cmd = [
            str(firefox_exe.resolve()),
            "-no-remote",
            "-profile",
            str(prof.resolve()),
        ]
        if not bool(show_browser):
            cmd.append("-headless")
        proc = subprocess.Popen(cmd, shell=False)
        _set_tiktok_manual_session(aid, _ManualSession(stop_event=threading.Event(), proc=proc))
        return
    chromium_exe = exe if exe.is_file() else _detect_chromium_exe()
    if chromium_exe is None:
        raise FileNotFoundError(
            "Không tìm thấy trình duyệt Chromium/Edge/Chrome trong máy. "
            "Hãy cài browser hoặc cấu hình browser_exe_path."
        )
    extra_args = _chromium_proxy_args(account)
    if not bool(show_browser):
        extra_args = [*extra_args, "--headless=new"]
    proc = open_chromium_like_profile(
        browser_exe=chromium_exe,
        profile_dir=prof,
        start_url="",
        extra_args=extra_args,
    )
    _set_tiktok_manual_session(aid, _ManualSession(stop_event=threading.Event(), proc=proc))
