from __future__ import annotations

import hashlib
import json
import os
import shutil
import sys
import uuid
import urllib.request
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from loguru import logger

from src.utils.app_restart import DEFERRED_GUI_BAT_NAME


@dataclass(frozen=True)
class UpdateManifest:
    """Manifest bản cập nhật lấy từ URL công khai."""

    version: str
    download_url: str
    sha256: str
    notes: str


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
    with zipfile.ZipFile(zip_path, "r") as zf:
        for info in zf.infolist():
            name = info.filename.replace("\\", "/")
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
    with urllib.request.urlopen(req, timeout=900) as resp, tmp_zip.open("wb") as fh:
        shutil.copyfileobj(resp, fh, length=4 * 1024 * 1024)

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

        defer_bat = updates_dir / DEFERRED_GUI_BAT_NAME
        if layout.exe_gui_root is not None and getattr(sys, "frozen", False) and os.name == "nt":
            _stage_deferred_exe_gui_merge_windows(
                exe_gui_root=layout.exe_gui_root,
                project_root=project_root,
                updates_dir=updates_dir,
                version=str(manifest.version),
                bat_out=defer_bat,
            )
            logger.info("Updater: đã stage exe_gui; sau relaunch chạy {}", defer_bat.name)
        else:
            defer_bat.unlink(missing_ok=True)
            for old in updates_dir.glob("staged_gui_*"):
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


def resolve_manifest_url(project_root: Path) -> str:
    """
    URL manifest update:
    - env ``TOOLFB_UPDATE_MANIFEST_URL``
    - hoặc ``config/update_channel.json``:
      - ``manifest_url`` (http/https)
      - ``manifest_file`` (đường dẫn tương đối tới project, dùng khi dev không có CDN)
    - fallback: ``dist/latest.json`` nếu file tồn tại (dev local sau khi build)
    """
    env_url = os.environ.get("TOOLFB_UPDATE_MANIFEST_URL", "").strip()
    if env_url:
        return env_url
    cf = project_root / "config" / "update_channel.json"
    if cf.is_file():
        try:
            raw = json.loads(cf.read_text(encoding="utf-8"))
        except Exception:
            raw = {}
        if isinstance(raw, dict):
            u = str(raw.get("manifest_url", "")).strip()
            if u:
                return u
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
    # Clone git thường chưa có update_channel.json: thử dò owner/repo từ ``git remote origin``.
    try:
        from src.utils.github_repo_detect import github_owner_repo_from_git

        r = github_owner_repo_from_git(project_root)
        if r:
            return github_latest_manifest_url(r)
    except Exception:
        pass
    return ""


def github_latest_manifest_url(owner_slash_repo: str) -> str:
    """
    Sinh URL manifest ``latest.json`` kiểu GitHub Releases (nhánh *latest/download*).

    Args:
        owner_slash_repo: Chuỗi ``owner/repo`` (ví dụ ``vanchien/ToolFB``).

    Returns:
        URL ``https://github.com/<owner>/<repo>/releases/latest/download/latest.json``.

    Raises:
        ValueError: Repo rỗng hoặc không đúng dạng ``owner/repo``.
    """
    r = (owner_slash_repo or "").strip().strip("/").replace(" ", "")
    if not r or "/" not in r:
        raise ValueError("Repo phải dạng owner/repo (ví dụ vanchien/ToolFB).")
    a, b, *rest = r.split("/", 2)
    if not a or not b or rest:
        raise ValueError("Repo phải dạng owner/repo (một dấu / giữa owner và tên repo).")
    return f"https://github.com/{a}/{b}/releases/latest/download/latest.json"
