#!/usr/bin/env python3
"""
Tự động hóa phát hành một lệnh (wrapper ``publish_all.py``).

Thực hiện tuần tự:
1. Bump / đặt ``version.json`` + build ``dist/ToolFB_release_bundle.zip`` + ``dist/latest.json``
2. Đồng bộ ``release/update/latest.json`` (trong ``build_release_bundle``)
3. ``gh release create`` kèm zip + ``latest.json``
4. Ghi ``config/update_channel.json`` (manifest raw) trừ khi ``--no-write-update-channel``

Yêu cầu: ``gh auth login`` (máy local) hoặc ``GH_TOKEN`` (CI).

Ví dụ::

  python tools/auto_release.py --skip-browser-bundle --notes "Sửa updater + manifest"

Tham số giống ``publish_all.py`` (được chuyển tiếp nguyên).
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    pub = root / "tools" / "publish_all.py"
    cmd = [sys.executable, str(pub), *sys.argv[1:]]
    return int(subprocess.call(cmd, cwd=str(root)))


if __name__ == "__main__":
    raise SystemExit(main())
