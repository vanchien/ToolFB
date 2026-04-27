"""
Dò ``owner/repo`` từ ``git remote get-url origin`` (GitHub).

Dùng cho GUI / bootstrap kênh cập nhật; không phụ thuộc GitHub CLI.
"""

from __future__ import annotations

import subprocess
from pathlib import Path


def parse_github_owner_repo(remote_url: str) -> str:
    """
    Trích ``owner/repo`` từ URL remote (HTTPS hoặc SSH).

    Returns:
        Chuỗi ``owner/repo`` hoặc rỗng nếu không nhận dạng được.
    """
    u = (remote_url or "").strip()
    if not u:
        return ""
    u = u.split()[0]
    u = u.rstrip("/")
    if u.endswith(".git"):
        u = u[:-4]
    if "git@github.com:" in u:
        tail = u.split("git@github.com:", 1)[1].strip()
        if "/" in tail:
            a, b = tail.split("/", 1)
            b = b.split("/")[0].split(":")[0]
            a, b = a.strip(), b.strip()
            if a and b:
                return f"{a}/{b}"
    low = u.lower()
    if "github.com/" in low:
        idx = low.index("github.com/") + len("github.com/")
        tail = u[idx:]
        parts = [p for p in tail.split("/") if p]
        if len(parts) >= 2:
            return f"{parts[0]}/{parts[1]}"
    return ""


def git_origin_url(project_root: Path) -> str:
    """Đọc URL ``origin``; rỗng nếu không có git hoặc lỗi."""
    try:
        p = subprocess.run(
            ["git", "remote", "get-url", "origin"],
            cwd=str(project_root),
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
        if p.returncode == 0 and (p.stdout or "").strip():
            return (p.stdout or "").strip()
    except FileNotFoundError:
        pass
    return ""


def github_owner_repo_from_git(project_root: Path) -> str:
    """
    ``owner/repo`` từ remote ``origin`` trỏ tới github.com.

    Returns:
        Chuỗi ``owner/repo`` hoặc rỗng.
    """
    return parse_github_owner_repo(git_origin_url(project_root))
