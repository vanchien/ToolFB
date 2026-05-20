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


def resolve_playwright_browsers_path(project_root: Path | None = None) -> Path | None:
    """Đường dẫn thư mục trình duyệt đang dùng (env hoặc bundle cạnh EXE)."""
    raw = os.environ.get("PLAYWRIGHT_BROWSERS_PATH", "").strip()
    if raw:
        p = Path(raw).expanduser()
        if p.is_dir():
            return p.resolve()
    root = Path(project_root) if project_root else None
    if root is None:
        from src.utils.paths import project_root as _pr

        root = _pr()
    if getattr(sys, "frozen", False):
        exe_dir = Path(sys.executable).resolve().parent
        for rel in ("_internal/ms-playwright", "ms-playwright"):
            cand = (exe_dir / rel).resolve()
            if cand.is_dir() and any(cand.iterdir()):
                return cand
    return None


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


def enforce_bundled_browser_policy(*, project_root: Path) -> tuple[bool, list[str]]:
    """
    Ép dùng Chromium bundle (không Chrome/Edge tự cập nhật) và kiểm tra khớp manifest.

    Returns:
        (ok, messages) — messages gồm cảnh báo/lỗi hiển thị GUI.
    """
    messages: list[str] = []
    frozen = getattr(sys, "frozen", False)
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
