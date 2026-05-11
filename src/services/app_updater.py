from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import uuid
import urllib.request
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from loguru import logger

from src.utils.app_restart import DEFERRED_GUI_BAT_NAME

# Repo công khai chứa ``release/update/latest.json`` (máy không có .git vẫn dùng manifest mặc định).
TOOLFB_PUBLIC_REPO = "vanchien/ToolFB"


def resolve_git_executable() -> str | None:
    """
    Tìm ``git`` thực thi: biến ``TOOLFB_GIT``, ``PATH``, rồi vị trí mặc định Git for Windows
    (mở GUI từ Explorer thường không có ``git`` trong PATH).
    """
    env_g = os.environ.get("TOOLFB_GIT", "").strip().strip('"')
    if env_g:
        p = Path(env_g)
        if p.is_file():
            return str(p.resolve())
    w = shutil.which("git")
    if w:
        return w
    if os.name == "nt":
        pf = os.environ.get("ProgramFiles", r"C:\Program Files")
        pfx86 = os.environ.get("ProgramFiles(x86)", r"C:\Program Files (x86)")
        for base in (pf, pfx86):
            if not base:
                continue
            for sub in ("Git/cmd/git.exe", "Git/bin/git.exe"):
                cand = Path(base) / sub
                if cand.is_file():
                    return str(cand.resolve())
    return None


@dataclass(frozen=True)
class UpdateManifest:
    """Manifest bản cập nhật lấy từ URL công khai."""

    version: str
    download_url: str
    sha256: str
    notes: str


@dataclass(frozen=True)
class GitUpdateCheckResult:
    """Kết quả ``git fetch`` + so sánh HEAD với tip trên ``origin``."""

    is_git_clone: bool
    git_on_path: bool
    ok: bool
    branch: str
    local_sha_short: str
    remote_sha_short: str
    remote_ref: str
    commits_behind: int
    remote_preview: str
    error: str | None

    @property
    def has_new_commits(self) -> bool:
        return self.ok and self.commits_behind > 0


def _git_run(
    project_root: Path,
    args: list[str],
    *,
    timeout: int = 120,
    git_exe: str | None = None,
) -> subprocess.CompletedProcess[str]:
    exe = git_exe or resolve_git_executable()
    if not exe:
        return subprocess.CompletedProcess(
            args=["git", *args],
            returncode=127,
            stdout="",
            stderr="Không tìm thấy git (PATH + Git for Windows). Đặt TOOLFB_GIT=đường_dẫn\\git.exe",
        )
    return subprocess.run(
        [exe, *args],
        cwd=str(project_root),
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
        encoding="utf-8",
        errors="replace",
    )


def should_use_git_updates(project_root: Path) -> bool:
    """
    Bản clone có ``.git`` + có thể gọi được ``git`` → ưu tiên «Kiểm tra / Cập nhật» bằng ``git pull``.
    Bản PyInstaller không dùng được.
    """
    if getattr(sys, "frozen", False):
        return False
    if not (project_root / ".git").exists():
        return False
    return resolve_git_executable() is not None


def _git_remote_tip_ref(project_root: Path, branch: str, *, git_exe: str | None = None) -> tuple[str, str] | None:
    """
    Returns:
        ``(full_sha, ref_label)`` với ref kiểu ``origin/main``, hoặc ``None``.
    """
    p = _git_run(project_root, ["rev-parse", f"origin/{branch}"], timeout=45, git_exe=git_exe)
    if p.returncode == 0 and (p.stdout or "").strip():
        sha = (p.stdout or "").strip()
        return sha, f"origin/{branch}"
    sym = _git_run(project_root, ["symbolic-ref", "-q", "refs/remotes/origin/HEAD"], timeout=45, git_exe=git_exe)
    if sym.returncode == 0 and (sym.stdout or "").strip():
        ref = (sym.stdout or "").strip()
        p3 = _git_run(project_root, ["rev-parse", ref], timeout=45, git_exe=git_exe)
        if p3.returncode == 0 and (p3.stdout or "").strip():
            return (p3.stdout or "").strip(), ref
    lr = _git_run(project_root, ["ls-remote", "--symref", "origin", "HEAD"], timeout=90, git_exe=git_exe)
    if lr.returncode == 0 and (lr.stdout or "").strip():
        first = (lr.stdout or "").strip().splitlines()[0]
        if first.startswith("ref:"):
            parts = first.split()
            if len(parts) >= 2:
                symref = parts[1].strip()
                if symref.startswith("refs/heads/"):
                    default_br = symref.split("/", 2)[2]
                    p4 = _git_run(project_root, ["rev-parse", f"origin/{default_br}"], timeout=45, git_exe=git_exe)
                    if p4.returncode == 0 and (p4.stdout or "").strip():
                        return (p4.stdout or "").strip(), f"origin/{default_br}"
    return None


def _resolve_remote_tip_after_fetch(
    project_root: Path, *, local_branch: str, git_exe: str | None = None
) -> tuple[str, str] | None:
    """
    Chọn ref trên ``origin`` để so sánh với ``HEAD``:
    1) ``@{upstream}`` nếu nhánh hiện tại đã gắn tracking;
    2) ``origin/<tên nhánh local>``;
    3) ``origin/HEAD`` (nhánh mặc định của remote).
    """
    up = _git_run(project_root, ["rev-parse", "--abbrev-ref", "@{u}"], timeout=30, git_exe=git_exe)
    if up.returncode == 0 and (up.stdout or "").strip():
        remote_ref = (up.stdout or "").strip()
        if remote_ref != "HEAD":
            tip = _git_run(project_root, ["rev-parse", remote_ref], timeout=30, git_exe=git_exe)
            if tip.returncode == 0 and (tip.stdout or "").strip():
                return (tip.stdout or "").strip(), remote_ref
    if local_branch:
        t2 = _git_remote_tip_ref(project_root, local_branch, git_exe=git_exe)
        if t2 is not None:
            return t2
    return _git_remote_tip_ref(project_root, "main", git_exe=git_exe)


def _commits_behind_left_right(project_root: Path, remote_ref: str, *, git_exe: str | None = None) -> int:
    """Số commit trên ``remote_ref`` mà ``HEAD`` chưa có (``left\\tright`` → lấy ``right``)."""
    lr = _git_run(
        project_root,
        ["rev-list", "--left-right", "--count", f"HEAD...{remote_ref}"],
        timeout=90,
        git_exe=git_exe,
    )
    if lr.returncode != 0 or not (lr.stdout or "").strip():
        return -1
    parts = (lr.stdout or "").strip().split()
    if len(parts) >= 2 and parts[1].isdigit():
        return int(parts[1])
    if "\t" in (lr.stdout or ""):
        seg = (lr.stdout or "").strip().split("\t", 1)
        if len(seg) == 2 and seg[1].strip().isdigit():
            return int(seg[1].strip())
    return -1


def check_git_updates(project_root: Path, *, timeout_fetch: int = 180) -> GitUpdateCheckResult:
    """``git fetch origin`` rồi đếm commit trên remote mà local chưa có."""
    git_exe = resolve_git_executable()
    if not (project_root / ".git").exists():
        return GitUpdateCheckResult(
            is_git_clone=False,
            git_on_path=git_exe is not None,
            ok=False,
            branch="",
            local_sha_short="",
            remote_sha_short="",
            remote_ref="",
            commits_behind=0,
            remote_preview="",
            error=None,
        )
    if not git_exe:
        return GitUpdateCheckResult(
            is_git_clone=True,
            git_on_path=False,
            ok=False,
            branch="",
            local_sha_short="",
            remote_sha_short="",
            remote_ref="",
            commits_behind=0,
            remote_preview="",
            error=(
                "Không tìm thấy git.exe (PATH và thư mục mặc định Git for Windows).\n"
                "Cài Git for Windows hoặc đặt biến môi trường TOOLFB_GIT trỏ tới git.exe "
                r"(vd. C:\Program Files\Git\cmd\git.exe)."
            ),
        )
    br_p = _git_run(project_root, ["branch", "--show-current"], timeout=30, git_exe=git_exe)
    branch = (br_p.stdout or "").strip()
    fe = _git_run(project_root, ["fetch", "origin"], timeout=timeout_fetch, git_exe=git_exe)
    if fe.returncode != 0:
        err = ((fe.stderr or "") + (fe.stdout or "")).strip() or "git fetch thất bại"
        return GitUpdateCheckResult(
            is_git_clone=True,
            git_on_path=True,
            ok=False,
            branch=branch or "(detached)",
            local_sha_short="",
            remote_sha_short="",
            remote_ref="",
            commits_behind=0,
            remote_preview="",
            error=err[:4000],
        )
    tip = _resolve_remote_tip_after_fetch(project_root, local_branch=branch, git_exe=git_exe)
    if tip is None:
        return GitUpdateCheckResult(
            is_git_clone=True,
            git_on_path=True,
            ok=False,
            branch=branch or "(detached)",
            local_sha_short="",
            remote_sha_short="",
            remote_ref="",
            commits_behind=0,
            remote_preview="",
            error=(
                "Không xác định được nhánh mặc định trên origin.\n"
                "Chạy: git remote -v và git branch -vv — cần origin trỏ đúng GitHub và đã fetch."
            ),
        )
    remote_full, remote_ref = tip
    loc = _git_run(project_root, ["rev-parse", "HEAD"], timeout=30, git_exe=git_exe)
    local_full = (loc.stdout or "").strip() if loc.returncode == 0 else ""
    behind = _commits_behind_left_right(project_root, remote_ref, git_exe=git_exe)
    if behind < 0:
        cnt = _git_run(project_root, ["rev-list", "--count", f"HEAD..{remote_full}"], timeout=60, git_exe=git_exe)
        if cnt.returncode == 0 and (cnt.stdout or "").strip().isdigit():
            behind = int((cnt.stdout or "").strip())
        elif local_full and remote_full and local_full != remote_full:
            behind = 1
        else:
            behind = 0
    log1 = _git_run(project_root, ["log", "-1", "--oneline", remote_full], timeout=30, git_exe=git_exe)
    preview = ((log1.stdout or "").strip())[:500]
    return GitUpdateCheckResult(
        is_git_clone=True,
        git_on_path=True,
        ok=True,
        branch=branch or "(detached)",
        local_sha_short=local_full[:12] if local_full else "",
        remote_sha_short=remote_full[:12],
        remote_ref=remote_ref,
        commits_behind=behind,
        remote_preview=preview,
        error=None,
    )


def apply_git_pull_ff(project_root: Path, *, result: GitUpdateCheckResult, timeout: int = 300) -> tuple[bool, str]:
    """
    Ưu tiên ``git pull --ff-only`` (theo upstream). Nếu lỗi: ``git pull --ff-only origin <nhánh>``
    với nhánh suy ra từ ``remote_ref`` (vd. ``origin/main`` → ``main``).
    """
    git_exe = resolve_git_executable()
    if not git_exe:
        return False, "Không tìm thấy git.exe."

    p = _git_run(project_root, ["pull", "--ff-only"], timeout=timeout, git_exe=git_exe)
    out = ((p.stdout or "").strip() + "\n" + (p.stderr or "").strip()).strip()
    if p.returncode == 0:
        return True, out

    remote_name = "origin"
    rb = "main"
    ref = (result.remote_ref or "").strip()
    if ref and "/" in ref:
        remote_name, rb = ref.split("/", 1)
    elif result.branch and result.branch != "(detached)":
        rb = result.branch

    p2 = _git_run(
        project_root,
        ["pull", "--ff-only", remote_name, rb],
        timeout=timeout,
        git_exe=git_exe,
    )
    out2 = ((p2.stdout or "").strip() + "\n" + (p2.stderr or "").strip()).strip()
    if p2.returncode == 0:
        return True, (out + "\n" + out2).strip() if out else out2
    return False, (out + "\n---\n" + out2).strip() if out else (out2 or "git pull --ff-only thất bại")


def read_local_version(project_root: Path) -> str:
    """Đọc phiên bản local từ ``version.json`` (fallback ``0.0.0-dev``)."""
    vf = project_root / "version.json"
    if not vf.is_file():
        return "0.0.0-dev"
    try:
        raw = json.loads(vf.read_text(encoding="utf-8"))
    except Exception:
        return "0.0.0-dev"
    if not isinstance(raw, dict):
        return "0.0.0-dev"
    return str(raw.get("version", "")).strip() or "0.0.0-dev"


def parse_github_owner_repo_from_url(url: str) -> str:
    """Từ URL GitHub (Release, repo…) → ``owner/repo``."""
    u = (url or "").strip()
    low = u.lower()
    if "github.com/" not in low:
        return ""
    idx = low.index("github.com/") + len("github.com/")
    rest = u[idx:].lstrip("/")
    parts = [p for p in rest.split("/") if p]
    if len(parts) >= 2:
        return f"{parts[0]}/{parts[1]}"
    return ""


def resolve_github_owner_repo_for_version_check(project_root: Path) -> str:
    """``owner/repo`` để đọc ``version.json`` raw: từ git hoặc URL manifest trong config."""
    try:
        from src.utils.github_repo_detect import github_owner_repo_from_git

        rid = github_owner_repo_from_git(project_root)
        if rid:
            return rid
    except Exception:
        pass
    cf = project_root / "config" / "update_channel.json"
    if cf.is_file():
        try:
            raw = json.loads(cf.read_text(encoding="utf-8"))
        except Exception:
            raw = {}
        if isinstance(raw, dict):
            u = str(raw.get("manifest_url", "")).strip()
            pr = parse_github_owner_repo_from_url(u)
            if pr:
                return pr
    return ""


def read_remote_version_from_github_raw(
    owner_repo: str,
    *,
    branches: tuple[str, ...] = ("main", "master"),
    timeout_sec: int = 15,
) -> tuple[str | None, str]:
    """
    Đọc ``version.json`` qua raw.githubusercontent.com (không cần git).

    Returns:
        ``(phiên_bản hoặc None, nhánh đã đọc được / ghi chú)``.
    """
    r = (owner_repo or "").strip().strip("/").replace(" ", "")
    if not r or "/" not in r:
        return None, ""
    for br in branches:
        url = f"https://raw.githubusercontent.com/{r}/{br}/version.json"
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "ToolFB-Updater/1.0"})
            with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
                data = resp.read()
            raw = json.loads(data.decode("utf-8", errors="replace"))
            if isinstance(raw, dict):
                v = str(raw.get("version", "")).strip()
                if v:
                    return v, br
        except Exception:
            continue
    return None, ""


def read_manifest_from_url(manifest_url: str, *, timeout_sec: int = 20) -> UpdateManifest:
    """Tải manifest JSON từ URL."""
    req = urllib.request.Request(manifest_url, headers={"User-Agent": "ToolFB-Updater/1.0"})
    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
        data = resp.read()
    raw = json.loads(data.decode("utf-8", errors="replace"))
    if not isinstance(raw, dict):
        raise ValueError("Manifest cập nhật không hợp lệ (không phải object).")
    version = str(raw.get("version", "")).strip()
    download_url = str(raw.get("download_url", "")).strip()
    sha256 = str(raw.get("sha256", "")).strip().lower()
    notes = str(raw.get("notes", "")).strip()
    if not version or not download_url:
        raise ValueError("Manifest thiếu version hoặc download_url.")
    return UpdateManifest(version=version, download_url=download_url, sha256=sha256, notes=notes)


def is_newer_version(remote_version: str, local_version: str) -> bool:
    """
    So sánh version kiểu semver đơn giản.
    Fallback: so sánh chuỗi khác nhau (nếu không parse được số).
    """
    def _nums(v: str) -> list[int]:
        out: list[int] = []
        for part in v.replace("-", ".").split("."):
            s = "".join(ch for ch in part if ch.isdigit())
            if s:
                out.append(int(s))
        return out

    rn = _nums(remote_version)
    ln = _nums(local_version)
    if rn and ln:
        m = max(len(rn), len(ln))
        rn = rn + [0] * (m - len(rn))
        ln = ln + [0] * (m - len(ln))
        return rn > ln
    return remote_version.strip() != local_version.strip()


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _validate_downloaded_zip(
    path: Path,
    *,
    declared_url: str,
    final_url: str | None,
) -> None:
    """
    GitHub / CDN đôi khi trả HTML (404, tên asset sai) thay vì file zip — ZipFile báo «not a zip file».
    Kiểm tra sớm để thông báo rõ cho người dùng và người phát hành manifest.
    """
    if not path.is_file():
        raise RuntimeError(f"Không có file tải về: {path}")
    sz = path.stat().st_size
    if sz < 64:
        extra = f"\nSau redirect: {final_url}" if final_url and final_url != declared_url.strip() else ""
        raise RuntimeError(
            f"Tải về chỉ {sz} byte — lỗi mạng hoặc URL không trả file ZIP.\n"
            f"URL trong manifest: {declared_url}{extra}\n"
            "Kiểm tra GitHub Release có đính kèm đúng tên file (ví dụ ToolFB_release_bundle.zip)."
        )
    head = path.read_bytes()[:1200]
    lead = head.lstrip()[:800]
    low = lead.lower()
    if low.startswith(b"<!doctype") or low.startswith(b"<html"):
        frag = lead.decode("utf-8", errors="replace")[:420].replace("\n", " ")
        extra = f"\nSau redirect: {final_url}" if final_url and final_url != declared_url.strip() else ""
        raise RuntimeError(
            "Tải về là trang HTML (thường 404 / asset không tồn tại trên Release), không phải file ZIP.\n"
            f"URL trong manifest: {declared_url}{extra}\n"
            "Cập nhật `download_url` trong latest.json trùng tên file trên Release «Latest».\n"
            f"Đầu nội dung: {frag!r}"
        )
    if len(head) < 4 or head[:2] != b"PK":
        frag_hex = head[:48].hex()
        extra = f"\nSau redirect: {final_url}" if final_url and final_url != declared_url.strip() else ""
        raise RuntimeError(
            f"File tải về không phải ZIP (không bắt đầu bằng PK). Kích thước {sz} byte.\n"
            f"URL trong manifest: {declared_url}{extra}\n"
            f"Hex đầu file: {frag_hex}"
        )


def _windows_long_path_str(path: Path) -> str:
    """Chuỗi đường dẫn Windows dài (\\\\?\\) để vượt MAX_PATH khi cần."""
    s = str(path.resolve())
    if os.name != "nt":
        return s
    if s.startswith("\\\\?\\"):
        return s
    if s.startswith("\\\\"):
        return "\\\\?\\UNC\\" + s[2:].lstrip("\\")
    return "\\\\?\\" + s


def _mkdir_for_long_path(path: Path) -> None:
    if os.name == "nt":
        lp = _windows_long_path_str(path)
        os.makedirs(lp, exist_ok=True)
        return
    path.mkdir(parents=True, exist_ok=True)


def _open_write_long_path(path: Path):
    if os.name == "nt":
        return open(_windows_long_path_str(path), "wb")
    return path.open("wb")


def _make_short_extract_root(*, project_root: Path, updates_dir: Path) -> Path:
    """
    Thư mục giải nén tạm càng ngắn càng tốt (Windows) để tránh vượt MAX_PATH trong zip sâu.

    Thử lần lượt: ``%SystemDrive%\\tfe\\``, ``%LOCALAPPDATA%\\tfe\\``, ``data/updates/``.
    """
    token = uuid.uuid4().hex[:12]
    bases: list[Path] = []
    if os.name == "nt":
        drv = (os.environ.get("SystemDrive") or "C:").rstrip("\\/") + "\\"
        bases.append(Path(drv) / "tfe")
        lad = os.environ.get("LOCALAPPDATA", "").strip()
        if lad:
            bases.append(Path(lad) / "tfe")
    bases.append(updates_dir)
    for b in bases:
        try:
            root = (b / f"e{token}").resolve()
            _mkdir_for_long_path(root)
            return root
        except OSError as exc:
            logger.warning("Updater: không dùng thư mục giải nén {}, thử tiếp: {}", b, exc)
    raise RuntimeError("Không tạo được thư mục giải nén tạm (hết chỗ ghi hoặc quyền).")


def _zip_member_should_skip_extract(member_name: str) -> bool:
    """
    Bỏ qua file cache Prisma/npm trong Veo3Studio — thường path cực dài, dễ lỗi extract,
    và có thể tải lại khi chạy server (Prisma tự tải engine).
    """
    norm = member_name.replace("\\", "/").lower()
    if "node_modules/.cache/prisma" in norm:
        return True
    if "node_modules/@prisma/engines/node_modules/.cache" in norm:
        return True
    return False


def _zip_extract_resilient(zip_path: Path, dest_dir: Path) -> tuple[int, int]:
    """
    Giải nén zip thủ công: chống ZIP slip, hỗ trợ đường dẫn dài Windows, bỏ qua member cache Prisma.

    Returns:
        (số file đã ghi, số member đã bỏ qua)
    """
    dest_dir = dest_dir.resolve()
    dest_dir.mkdir(parents=True, exist_ok=True)
    written = 0
    skipped = 0
    try:
        zf_ctx = zipfile.ZipFile(zip_path, "r")
    except zipfile.BadZipFile as exc:
        raise RuntimeError(
            f"Không đọc được file như ZIP (gói hỏng hoặc không phải zip): {zip_path}\n"
            "Xóa file trong data/updates/*.zip rồi bấm «Cập nhật ngay» lại; kiểm tra manifest / GitHub Release."
        ) from exc
    with zf_ctx as zf:
        for info in zf.infolist():
            name = info.filename.replace("\\", "/")
            norm = name.lower()
            if name.endswith("/") or not name.strip():
                continue
            if _zip_member_should_skip_extract(name):
                skipped += 1
                continue
            parts = name.split("/")
            if any(p == ".." or p.startswith(("/", "\\")) for p in parts):
                raise RuntimeError(f"Gói cập nhật chứa đường dẫn không an toàn: {name!r}")
            target = dest_dir.joinpath(*parts)
            try:
                target.relative_to(dest_dir)
            except ValueError as exc:
                raise RuntimeError(f"Gói cập nhật ZIP slip: {name!r}") from exc
            try:
                _mkdir_for_long_path(target.parent)
                with zf.open(info, "r") as src, _open_write_long_path(target) as out:
                    shutil.copyfileobj(src, out, length=1024 * 1024)
                written += 1
            except OSError as exc:
                win_e = int(getattr(exc, "winerror", 0) or 0)
                en = int(getattr(exc, "errno", 0) or 0)
                longish = len(_windows_long_path_str(target)) > 300 if os.name == "nt" else len(str(target)) > 240
                veo = "veo3studio" in norm
                if veo and (longish or win_e in {3, 206} or en in {2, 22, 36}):
                    logger.warning("Updater: bỏ qua member (path/IO): {} — {}", name[:180], exc)
                    skipped += 1
                    try:
                        if target.exists():
                            target.unlink(missing_ok=True)  # type: ignore[arg-type]
                    except OSError:
                        pass
                    continue
                raise
    if skipped:
        logger.info("Updater: đã bỏ qua {} file cache/ngoài giới hạn path khi giải nén.", skipped)
    logger.info("Updater: đã giải nén {} file vào {}", written, dest_dir)
    return written, skipped


@dataclass(frozen=True)
class UpdatePayloadLayout:
    """Bố cục gói giải nén: mã nguồn portable + (tuỳ chọn) thư mục bản EXE đóng gói."""

    code_root: Path
    exe_gui_root: Path | None


def _exe_gui_looks_valid(folder: Path) -> bool:
    return (folder / "ToolFB_GUI.exe").is_file() and (folder / "_internal").is_dir()


def _detect_update_payload_layout(extracted_root: Path) -> UpdatePayloadLayout:
    """
    Tìm thư mục mã nguồn cần copy và (nếu có) thư mục ``exe_gui`` cho bản PyInstaller.

    Thứ tự ưu tiên:
    - ``ToolFB_release_bundle/portable_clean`` (+ ``exe_gui`` cạnh đó)
    - ``portable_clean`` trực tiếp dưới gốc giải nén
    - gốc giải nén nếu đã là bản portable phẳng (legacy)
    """
    bundle = extracted_root / "ToolFB_release_bundle"
    pc_bundle = bundle / "portable_clean"
    eg_bundle = bundle / "exe_gui"
    if (pc_bundle / "main.py").is_file() and (pc_bundle / "src").is_dir():
        exe = eg_bundle if _exe_gui_looks_valid(eg_bundle) else None
        return UpdatePayloadLayout(code_root=pc_bundle, exe_gui_root=exe)

    pc2 = extracted_root / "portable_clean"
    if (pc2 / "main.py").is_file() and (pc2 / "src").is_dir():
        eg2 = extracted_root / "exe_gui"
        return UpdatePayloadLayout(code_root=pc2, exe_gui_root=eg2 if _exe_gui_looks_valid(eg2) else None)

    if (extracted_root / "main.py").is_file() and (extracted_root / "src").is_dir():
        eg3 = extracted_root / "exe_gui"
        return UpdatePayloadLayout(code_root=extracted_root, exe_gui_root=eg3 if _exe_gui_looks_valid(eg3) else None)

    for p in extracted_root.glob("**/main.py"):
        base = p.parent
        if (base / "src").is_dir():
            eg4 = base / "exe_gui"
            return UpdatePayloadLayout(code_root=base, exe_gui_root=eg4 if _exe_gui_looks_valid(eg4) else None)
    raise RuntimeError("Không tìm thấy payload cập nhật hợp lệ (thiếu main.py/src).")


def _merge_exe_gui_bundle(exe_gui: Path, project_root: Path) -> None:
    """Cập nhật ToolFB_GUI.exe + ``_internal`` từ gói release (non-Windows frozen hoặc công cụ ngoài)."""
    for name in ("ToolFB_GUI.exe",):
        src_f = exe_gui / name
        if src_f.is_file():
            shutil.copy2(src_f, project_root / name)
    internal_src = exe_gui / "_internal"
    internal_dst = project_root / "_internal"
    if internal_src.is_dir():
        if internal_dst.exists():
            shutil.rmtree(internal_dst, ignore_errors=True)
        internal_dst.mkdir(parents=True, exist_ok=True)
        _copytree_resilient(internal_src, internal_dst)


def _stage_deferred_exe_gui_merge_windows(
    *,
    exe_gui_root: Path,
    project_root: Path,
    updates_dir: Path,
    version: str,
    bat_out: Path,
) -> None:
    """
    Không ghi đè ``.exe``/``_internal`` khi process đang chạy (WinError 32).

    Sao chép vào ``staged_gui_*`` + tạo batch chạy sau khi user relaunch; batch đợi file nhả
    khóa rồi ``copy``/``robocopy`` rồi mở lại GUI và tự xóa.
    """
    for old in updates_dir.glob("staged_gui_*"):
        shutil.rmtree(old, ignore_errors=True)
    safe_ver = "".join(c if c.isalnum() or c in "-_" else "_" for c in version.strip())[:64] or "bundle"
    staged = updates_dir / f"staged_gui_{safe_ver}"
    staged.mkdir(parents=True, exist_ok=True)
    shutil.copy2(exe_gui_root / "ToolFB_GUI.exe", staged / "ToolFB_GUI.exe")
    _copytree_resilient(exe_gui_root / "_internal", staged / "_internal")

    pr_s = str(project_root.resolve())
    st_s = str(staged.resolve())
    exe_dst = str((project_root / "ToolFB_GUI.exe").resolve())

    bat_lines = [
        "@echo off",
        "setlocal",
        f'cd /d "{pr_s}"',
        "echo [ToolFB] Dang cap nhat ToolFB_GUI.exe va _internal...",
        ":L",
        "ping -n 2 127.0.0.1 >nul",
        f'copy /Y "{st_s}\\ToolFB_GUI.exe" "{exe_dst}" >nul 2>&1',
        "if errorlevel 1 goto L",
        f'robocopy "{st_s}\\_internal" "{pr_s}\\_internal" /MIR /R:3 /W:2 /NP',
        "if errorlevel 8 goto L",
        f'start "" "{exe_dst}" --gui',
        f'rd /s /q "{st_s}" 2>nul',
        'del "%~f0"',
        "endlocal",
        "",
    ]
    bat_out.write_text("\r\n".join(bat_lines), encoding="utf-8")


def _stage_deferred_full_apply_windows(
    *,
    payload_root: Path,
    exe_gui_root: Path | None,
    project_root: Path,
    updates_dir: Path,
    version: str,
    bat_out: Path,
    preserve_on_apply_dirs: tuple[str, ...],
) -> None:
    """
    Windows frozen: stage toàn bộ update và apply sau khi process hiện tại thoát.

    Mục tiêu: không còn copy/rmtree trực tiếp khi app đang chạy (tránh WinError 32).
    """
    for old in updates_dir.glob("staged_apply_*"):
        shutil.rmtree(old, ignore_errors=True)
    for old in updates_dir.glob("staged_gui_*"):
        shutil.rmtree(old, ignore_errors=True)
    safe_ver = "".join(c if c.isalnum() or c in "-_" else "_" for c in version.strip())[:64] or "bundle"
    staged = updates_dir / f"staged_apply_{safe_ver}"
    staged_payload = staged / "portable_apply"
    staged_payload.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(payload_root, staged_payload, dirs_exist_ok=True)

    if exe_gui_root is not None and _exe_gui_looks_valid(exe_gui_root):
        staged_gui = staged / "exe_gui"
        staged_gui.mkdir(parents=True, exist_ok=True)
        shutil.copy2(exe_gui_root / "ToolFB_GUI.exe", staged_gui / "ToolFB_GUI.exe")
        _copytree_resilient(exe_gui_root / "_internal", staged_gui / "_internal")

    pr_s = str(project_root.resolve())
    st_s = str(staged.resolve())
    exe_dst = str((project_root / "ToolFB_GUI.exe").resolve())
    excl = " ".join(preserve_on_apply_dirs)
    bat_lines = [
        "@echo off",
        "setlocal",
        f'cd /d "{pr_s}"',
        "echo [ToolFB] Dang apply update sau khi thoat app...",
        ":L",
        "ping -n 2 127.0.0.1 >nul",
        (
            f'robocopy "{st_s}\\portable_apply" "{pr_s}" /E /R:3 /W:2 /NP '
            f"/XD {excl}"
        ),
        "if errorlevel 8 goto L",
        f'if exist "{st_s}\\exe_gui\\ToolFB_GUI.exe" copy /Y "{st_s}\\exe_gui\\ToolFB_GUI.exe" "{exe_dst}" >nul 2>&1',
        f'if exist "{st_s}\\exe_gui\\ToolFB_GUI.exe" if errorlevel 1 goto L',
        f'if exist "{st_s}\\exe_gui\\_internal" robocopy "{st_s}\\exe_gui\\_internal" "{pr_s}\\_internal" /MIR /R:3 /W:2 /NP',
        f'if exist "{st_s}\\exe_gui\\_internal" if errorlevel 8 goto L',
        f'start "" "{exe_dst}" --gui',
        f'rd /s /q "{st_s}" 2>nul',
        'del "%~f0"',
        "endlocal",
        "",
    ]
    bat_out.write_text("\r\n".join(bat_lines), encoding="utf-8")


def apply_update_package(
    *,
    project_root: Path,
    manifest: UpdateManifest,
    backup_skip_dirs: tuple[str, ...] = ("data", ".venv", "logs", ".git", ".cursor", "dist", "build"),
    preserve_on_apply_dirs: tuple[str, ...] = (
        "data",
        ".venv",
        "logs",
        ".git",
        ".cursor",
        "dist",
        "build",
        "config",
    ),
) -> Path:
    """
    Tải và áp dụng gói cập nhật vào ``project_root``.

    Returns:
        Đường dẫn backup trước update.
    """
    updates_dir = project_root / "data" / "updates"
    updates_dir.mkdir(parents=True, exist_ok=True)
    tmp_zip = updates_dir / f"update_{manifest.version}.zip"
    logger.info("Updater: tải gói cập nhật từ {}", manifest.download_url)
    req = urllib.request.Request(manifest.download_url, headers={"User-Agent": "ToolFB-Updater/1.0"})
    final_url: str | None = None
    with urllib.request.urlopen(req, timeout=900) as resp, tmp_zip.open("wb") as fh:
        try:
            final_url = resp.geturl()
        except Exception:
            final_url = None
        if final_url:
            logger.info("Updater: URL sau redirect: {}", final_url)
        shutil.copyfileobj(resp, fh, length=4 * 1024 * 1024)

    _validate_downloaded_zip(
        tmp_zip,
        declared_url=manifest.download_url,
        final_url=final_url,
    )

    if manifest.sha256:
        got = _sha256_file(tmp_zip)
        if got.lower() != manifest.sha256.lower():
            raise RuntimeError(f"Sai checksum update package. expected={manifest.sha256} got={got}")

    # Giải nén ra thư mục tạm đường dẫn ngắn + extract thủ công (Windows path dài / bỏ cache Prisma).
    extract_root = _make_short_extract_root(project_root=project_root, updates_dir=updates_dir)
    try:
        _zip_extract_resilient(tmp_zip, extract_root)
        layout = _detect_update_payload_layout(extract_root)
        payload_root = layout.code_root
        defer_bat = updates_dir / DEFERRED_GUI_BAT_NAME

        # Windows frozen: không patch trực tiếp file đang chạy.
        if getattr(sys, "frozen", False) and os.name == "nt":
            backup_dir = updates_dir / f"backup_before_{manifest.version}"
            if backup_dir.exists():
                shutil.rmtree(backup_dir, ignore_errors=True)
            backup_dir.mkdir(parents=True, exist_ok=True)
            (backup_dir / "_deferred_apply_note.txt").write_text(
                "Deferred update mode: changes are staged and applied after app exits.\n",
                encoding="utf-8",
            )
            _stage_deferred_full_apply_windows(
                payload_root=payload_root,
                exe_gui_root=layout.exe_gui_root,
                project_root=project_root,
                updates_dir=updates_dir,
                version=str(manifest.version),
                bat_out=defer_bat,
                preserve_on_apply_dirs=preserve_on_apply_dirs,
            )
            logger.info("Updater: staged deferred apply; sau relaunch chạy {}", defer_bat.name)
            logger.info("Updater: áp dụng update {} thành công.", manifest.version)
            return backup_dir

        backup_dir = updates_dir / f"backup_before_{manifest.version}"
        if backup_dir.exists():
            shutil.rmtree(backup_dir, ignore_errors=True)
        backup_dir.mkdir(parents=True, exist_ok=True)

        backup_skip = set(backup_skip_dirs)
        for item in project_root.iterdir():
            if item.name in backup_skip:
                continue
            target = backup_dir / item.name
            if item.is_dir():
                shutil.copytree(item, target, dirs_exist_ok=True)
            else:
                shutil.copy2(item, target)

        # copy payload sang project root, bỏ qua data/venv/logs/config...
        # Riêng "tools" dùng copy resilient để luôn giữ Veo3Studio chạy được,
        # kể cả khi gặp file lẻ/symlink cache không còn tồn tại trong gói.
        preserve = set(preserve_on_apply_dirs)
        for item in payload_root.iterdir():
            if item.name in preserve:
                continue
            target = project_root / item.name
            if item.is_dir():
                if item.name == "tools":
                    _copytree_resilient(item, target)
                    continue
                if target.exists():
                    shutil.rmtree(target, ignore_errors=True)
                shutil.copytree(item, target, dirs_exist_ok=True)
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(item, target)

        defer_bat.unlink(missing_ok=True)
        for old in updates_dir.glob("staged_gui_*"):
            shutil.rmtree(old, ignore_errors=True)
        for old in updates_dir.glob("staged_apply_*"):
            shutil.rmtree(old, ignore_errors=True)
        if layout.exe_gui_root is not None and getattr(sys, "frozen", False) and os.name != "nt":
            _merge_exe_gui_bundle(layout.exe_gui_root, project_root)

        logger.info("Updater: áp dụng update {} thành công.", manifest.version)
        return backup_dir
    finally:
        shutil.rmtree(extract_root, ignore_errors=True)


def _copytree_resilient(src: Path, dst: Path) -> None:
    """
    Merge tree an toàn cho thư mục tools:
    - không fail toàn bộ chỉ vì 1 file cache/symlink thiếu.
    - vẫn copy phần còn lại để tool chạy được sau update.
    """
    src = src.resolve()
    dst.mkdir(parents=True, exist_ok=True)
    for root, dirs, files in os.walk(src):
        root_p = Path(root)
        rel = root_p.relative_to(src)
        out_dir = dst / rel
        out_dir.mkdir(parents=True, exist_ok=True)
        for d in dirs:
            try:
                (out_dir / d).mkdir(parents=True, exist_ok=True)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Updater tools: bỏ qua mkdir {}: {}", out_dir / d, exc)
        for f in files:
            s = root_p / f
            t = out_dir / f
            try:
                t.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(s, t)
            except FileNotFoundError:
                logger.warning("Updater tools: file nguồn không còn, bỏ qua {}", s)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Updater tools: không copy được {} -> {}: {}", s, t, exc)


def github_repo_raw_manifest_url(owner_slash_repo: str, *, branch: str = "main") -> str:
    """
    URL manifest trong repo (luôn theo commit trên nhánh), không phụ thuộc GitHub Release asset.

    File trong repo: ``release/update/latest.json`` (đồng bộ ``version`` với ``version.json``).
    """
    r = (owner_slash_repo or "").strip().strip("/").replace(" ", "")
    if not r or "/" not in r:
        raise ValueError("Repo phải dạng owner/repo (ví dụ vanchien/ToolFB).")
    a, b, *rest = r.split("/", 2)
    if not a or not b or rest:
        raise ValueError("Repo phải dạng owner/repo (một dấu / giữa owner và tên repo).")
    br = (branch or "main").strip() or "main"
    return f"https://raw.githubusercontent.com/{a}/{b}/{br}/release/update/latest.json"


def prefer_repo_raw_manifest_url(manifest_url: str) -> str:
    """
    Nếu manifest trỏ tới GitHub Release ``…/releases/latest/download/*.json`` (dễ cũ),
    chuyển sang manifest raw trên nhánh ``main`` trong cùng repo.
    """
    u = (manifest_url or "").strip()
    if not u:
        return u
    low = u.lower()
    if "raw.githubusercontent.com" in low and "release/update/latest.json" in low:
        return u
    rid = parse_github_owner_repo_from_url(u)
    if not rid:
        return u
    if "/releases/latest/download/" in low and low.rstrip("/").endswith(".json"):
        return github_repo_raw_manifest_url(rid)
    return u


def resolve_manifest_url(project_root: Path) -> str:
    """
    URL manifest update:
    - env ``TOOLFB_UPDATE_MANIFEST_URL``
    - hoặc ``config/update_channel.json``:
      - ``manifest_url`` (http/https) — URL kiểu Release ``latest/download`` được chuyển sang raw trên ``main``
      - ``manifest_file`` (đường dẫn tương đối tới project, dùng khi dev không có CDN)
    - fallback: ``dist/latest.json`` nếu file tồn tại (dev local sau khi build)
    - cuối: ``raw.githubusercontent.com/.../main/release/update/latest.json`` từ ``git remote origin``
    """
    env_url = os.environ.get("TOOLFB_UPDATE_MANIFEST_URL", "").strip()
    if env_url:
        return prefer_repo_raw_manifest_url(env_url)
    cf = project_root / "config" / "update_channel.json"
    if cf.is_file():
        try:
            raw = json.loads(cf.read_text(encoding="utf-8"))
        except Exception:
            raw = {}
        if isinstance(raw, dict):
            u = str(raw.get("manifest_url", "")).strip()
            if u:
                return prefer_repo_raw_manifest_url(u)
            mf_local = str(raw.get("manifest_file", "")).strip()
            if mf_local:
                p = Path(mf_local)
                if not p.is_absolute():
                    p = (project_root / p).resolve()
                if p.is_file():
                    return p.as_uri()
    dev_latest = (project_root / "dist" / "latest.json").resolve()
    if dev_latest.is_file():
        return dev_latest.as_uri()
    # Clone git thường chưa có update_channel.json: manifest raw trên nhánh main.
    try:
        from src.utils.github_repo_detect import github_owner_repo_from_git

        r = github_owner_repo_from_git(project_root)
        if r:
            return github_repo_raw_manifest_url(r)
    except Exception:
        pass
    return ""


def github_latest_manifest_url(owner_slash_repo: str) -> str:
    """
    Sinh URL manifest cập nhật (file ``release/update/latest.json`` trên nhánh ``main``).

    Giữ tên hàm để tương thích GUI; không còn trỏ tới Release ``latest/download`` (dễ lệch version).
    """
    return github_repo_raw_manifest_url(owner_slash_repo, branch="main")
