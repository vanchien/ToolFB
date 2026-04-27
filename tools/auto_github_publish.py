"""
Mot lenh (may phat hanh): day code + tao GitHub Release + ghi update_channel.

Chay o thu muc goc ToolFB (noi co .git)::

    python tools/auto_github_publish.py
    python tools/auto_github_publish.py --no-push
    python tools/auto_github_publish.py -- --bump minor --notes "Sua loi"

Can: ``gh`` da ``gh auth login``. Neu may khong co git/.git, truyen ``--repo owner/name``.

Moi tham so khong phai ``--no-push`` se duoc chuyen nguyen cho ``publish_all.py``.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path


def _root() -> Path:
    return Path(__file__).resolve().parents[1]


def _resolve_git_executable() -> str:
    """Tìm đường dẫn git executable; trả rỗng nếu không thấy."""
    raw = (os.environ.get("GIT_BIN") or "").strip().strip('"')
    if raw and Path(raw).is_file():
        return str(Path(raw).resolve())
    which = shutil.which("git")
    if which:
        return which
    if os.name == "nt":
        pf = os.environ.get("ProgramFiles", r"C:\Program Files")
        lad = os.environ.get("LOCALAPPDATA", "")
        for c in (
            Path(pf) / "Git" / "cmd" / "git.exe",
            Path(pf) / "Git" / "bin" / "git.exe",
            Path(lad) / "Programs" / "Git" / "cmd" / "git.exe" if lad else Path(),
        ):
            if c and c.is_file():
                return str(c.resolve())
    return ""


def _git_has_changes(git_bin: str, *, root: Path) -> bool:
    """True nếu working tree có thay đổi (tracked/untracked)."""
    p = subprocess.run(
        [git_bin, "status", "--porcelain"],
        cwd=str(root),
        capture_output=True,
        text=True,
        check=False,
    )
    return bool((p.stdout or "").strip())


def _auto_commit_if_needed(git_bin: str, *, root: Path, message: str) -> None:
    """Tự add + commit khi có thay đổi; không lỗi nếu không có gì để commit."""
    if not _git_has_changes(git_bin, root=root):
        print("INFO: khong co thay doi git, bo qua commit.")
        return
    subprocess.run([git_bin, "add", "-A"], cwd=str(root), check=True)
    p = subprocess.run(
        [git_bin, "commit", "-m", message],
        cwd=str(root),
        check=False,
    )
    if p.returncode != 0:
        print(
            "CANH BAO: auto-commit that bai (co the do hook/khong co thay doi hop le).",
            file=sys.stderr,
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="git push (optional) + python tools/publish_all.py [args...]",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--no-push",
        action="store_true",
        help="Bo qua buoc git push (chi build + tao release).",
    )
    parser.add_argument(
        "--no-auto-commit",
        action="store_true",
        help="Bo qua auto-commit truoc khi push.",
    )
    parser.add_argument(
        "--commit-message",
        default="",
        help="Message commit khi auto-commit (mac dinh tu dong theo thoi gian).",
    )
    parser.add_argument(
        "--repo",
        default="",
        help="GitHub repo owner/name (vi du vanchien/ToolFB) de truyen sang publish_all.py.",
    )
    args, publish_rest = parser.parse_known_args()

    extra = list(publish_rest)
    if extra and extra[0] == "--":
        extra = extra[1:]
    if args.repo.strip():
        has_repo = any(t == "--repo" or t.startswith("--repo=") for t in extra)
        if not has_repo:
            extra = ["--repo", args.repo.strip(), *extra]

    root = _root()
    if not args.no_push:
        git_bin = _resolve_git_executable()
        if not git_bin:
            print(
                "CANH BAO: khong tim thay git.exe (PATH/GIT_BIN). Bo qua git push, tiep tuc publish...",
                file=sys.stderr,
            )
        else:
            if not args.no_auto_commit:
                cm = str(args.commit_message).strip() or f"chore: auto publish {datetime.now().strftime('%Y-%m-%d %H:%M')}"
                _auto_commit_if_needed(git_bin, root=root, message=cm)
            r = subprocess.run(
                [git_bin, "push", "-u", "origin", "HEAD"],
                cwd=str(root),
            )
            if r.returncode != 0:
                print(
                    "CANH BAO: git push that bai (kiem tra remote / quyen). Tiep tuc publish local...",
                    file=sys.stderr,
                )

    cmd = [sys.executable, str(root / "tools" / "publish_all.py")] + extra
    rc = int(subprocess.call(cmd, cwd=str(root)))
    if rc != 0:
        print(
            "GOI Y: neu gh khong co trong PATH, dat GH_BIN, vi du: "
            "set GH_BIN=C:\\Program Files\\GitHub CLI\\gh.exe",
            file=sys.stderr,
        )
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
