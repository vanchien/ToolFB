#!/usr/bin/env python3
"""Ghi ``browser_bundle_manifest.json`` sau khi bundle Playwright (build release)."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.utils.playwright_browser_lock import (  # noqa: E402
    build_browser_manifest,
    scan_browser_folders,
    write_browser_manifest_file,
)


def _read_app_version(root: Path) -> str:
    vf = root / "version.json"
    if not vf.is_file():
        return ""
    try:
        raw = json.loads(vf.read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            return str(raw.get("version") or "").strip()
    except (OSError, json.JSONDecodeError):
        pass
    return ""


def main() -> int:
    parser = argparse.ArgumentParser(description="Ghi manifest khóa trình duyệt Playwright.")
    parser.add_argument(
        "--browsers-path",
        required=True,
        help="Thư mục ms-playwright (vd. dist/ToolFB_GUI/_internal/ms-playwright).",
    )
    parser.add_argument("--app-version", default="", help="Ghi đè version app (mặc định đọc version.json).")
    parser.add_argument(
        "--out",
        action="append",
        default=[],
        help="File đích (có thể chỉ định nhiều lần).",
    )
    args = parser.parse_args()
    root = _ROOT
    browsers = Path(args.browsers_path).expanduser().resolve()
    if not browsers.is_dir():
        print(f"ERROR: không thấy thư mục browsers: {browsers}", file=sys.stderr)
        return 1
    app_v = str(args.app_version or "").strip() or _read_app_version(root)
    payload = build_browser_manifest(app_version=app_v, browsers_root=browsers)
    if not payload.get("browsers"):
        print(f"ERROR: không quét được browser trong {browsers}", file=sys.stderr)
        return 2
    outs = [Path(p) for p in args.out] if args.out else [
        root / "release" / "browser_bundle_manifest.json",
    ]
    for op in outs:
        write_browser_manifest_file(op, payload)
        print(f"BROWSER_MANIFEST_WRITTEN={op}")
    print("BROWSERS=" + ", ".join(f"{k}={v}" for k, v in sorted(payload["browsers"].items())))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
