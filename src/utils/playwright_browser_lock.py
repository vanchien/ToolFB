"""
Khóa phiên bản trình duyệt Playwright theo bản build ToolFB.

Mục tiêu: máy chính và máy khách dùng cùng Chromium/Firefox/WebKit (cùng revision),
không tự ``playwright install`` hoặc Chrome hệ thống lệch bản.
"""

from __future__ import annotations

import json
import os
import re
import sys
from importlib.metadata import PackageNotFoundError, version as pkg_version
from pathlib import Path
from typing import Any

from loguru import logger

_MANIFEST_NAME = "browser_bundle_manifest.json"
_BROWSER_DIR_RE = re.compile(
    r"^(chromium_headless_shell|chromium|firefox|webkit|ffmpeg|winldd)-\d+$"
)


def playwright_python_version() -> str:
    try:
        return str(pkg_version("playwright")).strip()
    except PackageNotFoundError:
        return ""


def scan_browser_folders(browsers_root: Path) -> dict[str, str]:
    """Đọc tên thư mục con trong ``ms-playwright`` (vd. chromium-1208)."""
    out: dict[str, str] = {}
    if not browsers_root.is_dir():
        return out
    try:
        names = sorted(p.name for p in browsers_root.iterdir() if p.is_dir())
    except OSError:
        return out
    for name in names:
        if not _BROWSER_DIR_RE.match(name):
            continue
        key = name.rsplit("-", 1)[0]
        out[key] = name
    return out


def build_browser_manifest(*, app_version: str, browsers_root: Path) -> dict[str, Any]:
    return {
        "app_version": str(app_version or "").strip(),
        "playwright_python": playwright_python_version(),
        "browsers": scan_browser_folders(browsers_root),
    }


def write_browser_manifest_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def find_manifest_file(project_root: Path) -> Path | None:
    root = Path(project_root).resolve()
    candidates = [
        root / "_internal" / _MANIFEST_NAME,
        root / _MANIFEST_NAME,
        root / "release" / _MANIFEST_NAME,
    ]
    if getattr(sys, "frozen", False):
        exe_dir = Path(sys.executable).resolve().parent
        candidates = [
            exe_dir / "_internal" / _MANIFEST_NAME,
            exe_dir / _MANIFEST_NAME,
            *candidates,
        ]
    for p in candidates:
        if p.is_file():
            return p
    return None


def load_browser_manifest(project_root: Path) -> dict[str, Any] | None:
    p = find_manifest_file(project_root)
    if not p:
        return None
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Không đọc được {}: {}", p, exc)
        return None
    return raw if isinstance(raw, dict) else None


def bundled_browsers_dir_near_exe(exe_dir: Path | None = None) -> Path | None:
    """Thư mục ``ms-playwright`` cạnh EXE (không phụ thuộc máy người dùng / cache hệ thống)."""
    base = Path(exe_dir) if exe_dir else Path(sys.executable).resolve().parent
    for rel in ("_internal/ms-playwright", "ms-playwright"):
        cand = (base / rel).resolve()
        if cand.is_dir() and any(cand.iterdir()):
            return cand
    return None


def _sanitize_playwright_browsers_path_env(*, app_root: Path) -> None:
    """
    Bỏ ``PLAYWRIGHT_BROWSERS_PATH`` nếu trỏ cache máy khác / đường dẫn không tồn tại.

    Tránh lỗi «Executable doesn't exist» khi env còn từ máy dev hoặc ``playwright install`` cũ.
    """
    raw = os.environ.get("PLAYWRIGHT_BROWSERS_PATH", "").strip()
    if not raw:
        return
    p = Path(raw).expanduser()
    if not p.is_dir():
        os.environ.pop("PLAYWRIGHT_BROWSERS_PATH", None)
        logger.warning(
            "Đã xóa PLAYWRIGHT_BROWSERS_PATH (không tồn tại): {} — dùng trình duyệt đi kèm app.",
            raw,
        )
        return
    if getattr(sys, "frozen", False) and not _is_under_app_dir(p, app_root):
        bundled = bundled_browsers_dir_near_exe(app_root)
        if bundled is not None:
            os.environ.pop("PLAYWRIGHT_BROWSERS_PATH", None)
            logger.warning(
                "Đã xóa PLAYWRIGHT_BROWSERS_PATH trỏ ngoài thư mục cài ({}) — khóa bundle: {}.",
                p,
                bundled,
            )


def resolve_playwright_browsers_path(project_root: Path | None = None) -> Path | None:
    """Đường dẫn thư mục trình duyệt (ưu tiên bundle cạnh EXE, không dùng cache máy lạ)."""
    root = Path(project_root) if project_root else None
    if root is None:
        from src.utils.paths import project_root as _pr

        root = _pr()
    _sanitize_playwright_browsers_path_env(app_root=root)
    if getattr(sys, "frozen", False):
        bundled = bundled_browsers_dir_near_exe(root)
        if bundled is not None:
            return bundled
    raw = os.environ.get("PLAYWRIGHT_BROWSERS_PATH", "").strip()
    if raw:
        p = Path(raw).expanduser()
        if p.is_dir():
            if getattr(sys, "frozen", False) and _is_under_app_dir(p, root):
                return p.resolve()
            if not getattr(sys, "frozen", False):
                return p.resolve()
    if getattr(sys, "frozen", False):
        return bundled_browsers_dir_near_exe(root)
    return bundled_browsers_dir_near_exe(root) if (root / "_internal" / "ms-playwright").is_dir() else None


def _is_under_app_dir(path: Path, app_root: Path) -> bool:
    try:
        path.resolve().relative_to(app_root.resolve())
        return True
    except ValueError:
        return False


def validate_browser_bundle(
    *,
    project_root: Path,
    manifest: dict[str, Any] | None = None,
    browsers_path: Path | None = None,
) -> list[str]:
    """
  Trả về danh sách lỗi tiếng Việt (rỗng = khớp manifest).
    """
    errors: list[str] = []
    mf = manifest if manifest is not None else load_browser_manifest(project_root)
    if not mf:
        return errors
    expected = mf.get("browsers")
    if not isinstance(expected, dict) or not expected:
        errors.append("Manifest trình duyệt thiếu mục «browsers».")
        return errors
    bp = browsers_path or resolve_playwright_browsers_path(project_root)
    if bp is None or not bp.is_dir():
        errors.append(
            "Không tìm thấy thư mục trình duyệt Playwright đi kèm app "
            "(cần _internal/ms-playwright trong bản cài đặt). "
            "Không chạy «playwright install» riêng trên máy khách."
        )
        return errors
    actual = scan_browser_folders(bp)
    for key, exp_name in expected.items():
        exp_s = str(exp_name or "").strip()
        if not exp_s:
            continue
        act_s = str(actual.get(key) or "").strip()
        if not act_s:
            errors.append(f"Thiếu {key}: cần «{exp_s}» trong {bp}.")
        elif act_s != exp_s:
            errors.append(
                f"Lệch {key}: cần «{exp_s}» (bản build), máy này có «{act_s}». "
                f"Cập nhật đủ bản zip ToolFB từ máy chính — không tự cập nhật trình duyệt."
            )
    exp_app = str(mf.get("app_version") or "").strip()
    if exp_app:
        try:
            from src.services.app_updater import read_local_version

            local_v = read_local_version(project_root)
        except Exception:
            local_v = ""
        if local_v and exp_app != local_v:
            errors.append(
                f"Manifest trình duyệt gắn app {exp_app} nhưng đang chạy {local_v} — "
                f"cần cài đúng bản release khớp bundle."
            )
    return errors


def browser_executable_missing_message(
    *,
    browser_key: str,
    project_root: Path | None = None,
) -> str:
    """Thông báo lỗi tiếng Việt khi thiếu firefox/chromium trong bundle."""
    from src.utils.paths import project_root as _pr

    proot = project_root or _pr()
    bp = resolve_playwright_browsers_path(proot)
    mf = load_browser_manifest(proot)
    exp = ""
    if mf and isinstance(mf.get("browsers"), dict):
        exp = str(mf["browsers"].get(browser_key) or "").strip()
    lines = [
        f"Thiếu trình duyệt {browser_key} trong bản cài ToolFB.",
        f"Thư mục cài: {proot}",
    ]
    if bp:
        lines.append(f"Đã tìm: {bp}")
    else:
        lines.append("Không có _internal/ms-playwright cạnh ToolFB_GUI.exe.")
    if exp:
        lines.append(f"Cần gói: {exp} (khóa theo manifest — không tự cập nhật).")
    lines.extend(
        [
            "→ Tải bản release ĐẦY ĐỦ từ GitHub (zip có kèm trình duyệt, ~hàng trăm MB).",
            "→ Giải nén cả thư mục exe_gui, không chỉ copy file .exe.",
            "→ Không chạy «playwright install» trên máy khách (sẽ lệch phiên bản).",
        ]
    )
    return "\n".join(lines)


def assert_browsers_ready_for_launch(*, project_root: Path, browser_key: str) -> None:
    """Ném ``RuntimeError`` nếu bundle thiếu hoặc lệch manifest trước khi mở browser."""
    ok, msgs = enforce_bundled_browser_policy(project_root=project_root)
    if not ok:
        raise RuntimeError("\n".join(msgs) or browser_executable_missing_message(browser_key=browser_key))
    bp = resolve_playwright_browsers_path(project_root)
    mf = load_browser_manifest(project_root)
    if not bp or not mf:
        if getattr(sys, "frozen", False):
            raise RuntimeError(browser_executable_missing_message(browser_key=browser_key))
        return
    folder = str((mf.get("browsers") or {}).get(browser_key) or "").strip()
    if not folder:
        return
    exe_name = "firefox.exe" if browser_key == "firefox" else "chrome.exe"
    if browser_key == "chromium":
        cand = bp / folder / "chrome-win64" / exe_name
        if not cand.is_file():
            cand = bp / folder / "chrome-win" / exe_name
    elif browser_key == "firefox":
        cand = bp / folder / "firefox" / exe_name
    else:
        return
    if not cand.is_file():
        raise RuntimeError(browser_executable_missing_message(browser_key=browser_key))


def enforce_bundled_browser_policy(*, project_root: Path) -> tuple[bool, list[str]]:
    """
    Ép dùng Chromium bundle (không Chrome/Edge tự cập nhật) và kiểm tra khớp manifest.

    Returns:
        (ok, messages) — messages gồm cảnh báo/lỗi hiển thị GUI.
    """
    messages: list[str] = []
    frozen = getattr(sys, "frozen", False)
    _sanitize_playwright_browsers_path_env(app_root=project_root)
    bp = resolve_playwright_browsers_path(project_root)

    if frozen and bp is None:
        messages.append(
            "Bản cài (.exe) thiếu _internal/ms-playwright. "
            "Tải bản release ĐẦY ĐỦ (có kèm trình duyệt) từ máy chính — "
            "không dùng bản build «skip-browser-bundle» trên máy khách."
        )
        return False, messages

    if bp is not None:
        os.environ["PLAYWRIGHT_BROWSERS_PATH"] = str(bp)
        os.environ["FB_PLAYWRIGHT_CHROMIUM_CHANNEL"] = "bundled"
        os.environ.setdefault("TOOLFB_ENFORCE_BUNDLED_BROWSER", "1")
        logger.info("Playwright browsers path (khóa bundle): {}", bp)
    elif frozen:
        os.environ["FB_PLAYWRIGHT_CHROMIUM_CHANNEL"] = "bundled"

    mf = load_browser_manifest(project_root)
    if mf and bp:
        app_root = Path(project_root).resolve()
        if frozen and not _is_under_app_dir(bp, app_root):
            messages.append(
                f"PLAYWRIGHT_BROWSERS_PATH đang trỏ ngoài thư mục app ({bp}). "
                f"Xóa biến môi trường đó để dùng trình duyệt đi kèm bản cài, "
                f"tránh lệch phiên bản với máy chính."
            )
        val_errs = validate_browser_bundle(
            project_root=project_root, manifest=mf, browsers_path=bp
        )
        messages.extend(val_errs)
        if val_errs:
            return False, messages

    if frozen:
        os.environ.setdefault("FB_PLAYWRIGHT_CHROMIUM_CHANNEL", "bundled")
        os.environ.setdefault("TOOLFB_ENFORCE_BUNDLED_BROWSER", "1")

    return True, messages


def format_browser_status_lines(*, project_root: Path) -> list[str]:
    """Dòng trạng thái ngắn cho log / GUI."""
    lines: list[str] = []
    bp = resolve_playwright_browsers_path(project_root)
    mf = load_browser_manifest(project_root)
    if bp:
        lines.append(f"Trình duyệt: {bp}")
        scanned = scan_browser_folders(bp)
        if scanned:
            lines.append("Gói: " + ", ".join(f"{k}={v}" for k, v in sorted(scanned.items())))
    else:
        lines.append("Trình duyệt: (chưa có bundle — cache hệ thống hoặc thiếu cài đặt)")
    if mf:
        lines.append(
            f"Manifest build: app {mf.get('app_version', '?')} | "
            f"playwright {mf.get('playwright_python', '?')}"
        )
    ch = os.environ.get("FB_PLAYWRIGHT_CHROMIUM_CHANNEL", "")
    if ch:
        lines.append(f"Chromium channel: {ch}")
    return lines
