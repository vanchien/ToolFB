"""
Entry nhanh: chay publish_all (build + GitHub Release + ghi update_channel).

Vi du::
    python tools/publish_github_auto.py
    python tools/publish_github_auto.py --bump minor --notes "Sua updater"
"""

from __future__ import annotations

import runpy
import sys
from pathlib import Path


def main() -> None:
    target = Path(__file__).resolve().parent / "publish_all.py"
    sys.argv[0] = str(target)
    runpy.run_path(str(target), run_name="__main__")


if __name__ == "__main__":
    main()
