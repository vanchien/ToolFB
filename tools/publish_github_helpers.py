"""Hỗ trợ publish lên GitHub: dò repo, kiểm tra gh CLI."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path


def _ensure_project_root_on_path() -> Path:
    root = Path(__file__).resolve().parents[1]
    s = str(root)
    if s not in sys.path:
        sys.path.insert(0, s)
    return root


_ensure_project_root_on_path()

from src.utils.github_repo_detect import github_owner_repo_from_git


def resolve_gh_executable() -> str:
    """
    Đường dẫn tới ``gh.exe`` (GitHub CLI).

    Thứ tự: biến ``GH_BIN``, ``PATH`` (``shutil.which``), rồi vị trí cài mặc định trên Windows.
    """
    raw = (os.environ.get("GH_BIN") or "").strip().strip('"')
    if raw and Path(raw).is_file():
        return str(Path(raw).resolve())
    which = shutil.which("gh")
    if which:
        return which
    if os.name == "nt":
        pf = os.environ.get("ProgramFiles", r"C:\Program Files")
        lad = os.environ.get("LOCALAPPDATA", "")
        for c in (
            Path(pf) / "GitHub CLI" / "gh.exe",
            Path(r"C:\Program Files\GitHub CLI\gh.exe"),
            Path(lad) / "Programs" / "GitHub CLI" / "gh.exe" if lad else Path(),
        ):
            if c and c.is_file():
                return str(c.resolve())
    raise FileNotFoundError(
        "Không tìm thấy GitHub CLI (gh). Cài GitHub CLI, thêm vào PATH, "
        "hoặc đặt biến môi trường GH_BIN=đường_dẫn\\gh.exe"
    )


def gh_cli(*args: str) -> list[str]:
    """Lệnh đầy đủ: [đường_gh, *args]."""
    return [resolve_gh_executable(), *args]


def detect_github_repo(project_root: Path) -> str:
    """Từ ``git remote origin`` (GitHub), hoặc ``gh repo view`` nếu có."""
    got = github_owner_repo_from_git(project_root)
    if got:
        return got

    try:
        p = subprocess.run(
            gh_cli("repo", "view", "--json", "nameWithOwner", "-q", ".nameWithOwner"),
            cwd=str(project_root),
            capture_output=True,
            text=True,
            timeout=25,
            check=False,
        )
        if p.returncode == 0:
            line = (p.stdout or "").strip()
            if line and "/" in line:
                return line
    except FileNotFoundError:
        pass
    return ""


def ensure_gh_authenticated(project_root: Path) -> None:
    """Kiểm tra đã ``gh auth login`` (hoặc có ``GH_TOKEN`` trong CI)."""
    subprocess.run(gh_cli("auth", "status"), cwd=str(project_root), check=True)
