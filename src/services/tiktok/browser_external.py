from __future__ import annotations

import os
import subprocess
from shutil import which
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from src.services.tiktok.layout import ensure_tiktok_layout


def open_chromium_like_profile(
    *,
    browser_exe: Path,
    profile_dir: Path,
    start_url: str,
    extra_args: list[str] | None = None,
) -> None:
    """
    Mở Chrome/Chromium/Edge kiểu portable với ``--user-data-dir`` (đăng nhập thủ công, không giữ Playwright).
    """
    if not browser_exe.is_file():
        raise FileNotFoundError(f"Không tìm thấy browser: {browser_exe}")
    profile_dir.mkdir(parents=True, exist_ok=True)
    cmd = [str(browser_exe.resolve()), f"--user-data-dir={str(profile_dir.resolve())}"]
    if extra_args:
        cmd.extend([str(x) for x in extra_args if str(x).strip()])
    cmd.append(start_url)
    subprocess.Popen(cmd, shell=False)


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
    return _first_existing_file(cands)


def open_tiktok_profile_for_manual_login(account: dict[str, Any]) -> None:
    exe = Path(str(account.get("browser_exe_path", "")).strip())
    prof_s = str(account.get("profile_path", "")).strip()
    if not prof_s:
        aid = str(account.get("id", "")).strip() or "default"
        prof_s = str((ensure_tiktok_layout()["root"] / "profiles" / aid).resolve())
    prof = Path(prof_s)
    bt = str(account.get("browser_type", "chrome") or "chrome").strip().lower()
    if bt == "firefox":
        prof.mkdir(parents=True, exist_ok=True)
        _write_firefox_proxy_user_js(prof, account)
        firefox_exe = exe if exe.is_file() else _detect_firefox_exe()
        if firefox_exe is None:
            raise FileNotFoundError(
                "Không tìm thấy Firefox executable tự động. "
                "Hãy cài Firefox (Program Files) hoặc điền browser_exe_path trỏ tới firefox.exe."
            )
        if not firefox_exe.is_file():
            raise FileNotFoundError(
                "Không tìm thấy Firefox executable. Cài Firefox hoặc cấu hình browser_exe_path trỏ tới firefox.exe."
            )
        subprocess.Popen(
            [
                str(firefox_exe.resolve()),
                "-no-remote",
                "-profile",
                str(prof.resolve()),
                "https://www.tiktok.com/",
            ],
            shell=False,
        )
        return
    chromium_exe = exe if exe.is_file() else _detect_chromium_exe()
    if chromium_exe is None:
        raise FileNotFoundError(
            "Không tìm thấy trình duyệt Chromium/Edge/Chrome trong máy. "
            "Hãy cài browser hoặc cấu hình browser_exe_path."
        )
    open_chromium_like_profile(
        browser_exe=chromium_exe,
        profile_dir=prof,
        start_url="https://www.tiktok.com/",
        extra_args=_chromium_proxy_args(account),
    )
