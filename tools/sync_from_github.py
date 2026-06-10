"""
Đồng bộ code từ GitHub (``git pull --ff-only``) — chạy tay hoặc Task Scheduler.

Ví dụ:
  .venv\\Scripts\\python.exe tools\\sync_from_github.py
  .venv\\Scripts\\python.exe tools\\sync_from_github.py --force

Biến môi trường: ``TOOLFB_AUTO_GIT_PULL=0`` tắt; ``TOOLFB_AUTO_GIT_PULL=1`` bật.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.services.app_updater import maybe_auto_git_pull_on_startup  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="ToolFB — git pull từ origin (ff-only).")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Bỏ qua khoảng chờ min_interval_minutes giữa các lần pull.",
    )
    args = parser.parse_args()
    outcome = maybe_auto_git_pull_on_startup(_ROOT, force=bool(args.force))
    print(outcome.message)
    if outcome.pulled:
        return 0
    if outcome.skipped_reason in ("up_to_date", "disabled", "not_git_clone"):
        return 0
    if outcome.skipped_reason in ("dirty_worktree", "min_interval", "check_failed", "pull_failed"):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
