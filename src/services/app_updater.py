from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
import urllib.request
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from loguru import logger


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


def _detect_update_payload_root(extracted_root: Path) -> Path:
    """
    Tìm thư mục payload cần copy:
    - ưu tiên release bundle: ``ToolFB_release_bundle/portable_clean``
    - nếu không có thì dùng thư mục chứa ``main.py``.
    """
    candidates = [
        extracted_root / "ToolFB_release_bundle" / "portable_clean",
        extracted_root / "portable_clean",
        extracted_root,
    ]
    for c in candidates:
        if (c / "main.py").is_file() and (c / "src").is_dir():
            return c
    # quét 2 cấp
    for p in extracted_root.glob("**/main.py"):
        base = p.parent
        if (base / "src").is_dir():
            return base
    raise RuntimeError("Không tìm thấy payload cập nhật hợp lệ (thiếu main.py/src).")


def apply_update_package(
    *,
    project_root: Path,
    manifest: UpdateManifest,
    keep_dirs: tuple[str, ...] = ("data", ".venv", "logs", ".git", ".cursor", "dist", "build"),
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
    with urllib.request.urlopen(req, timeout=120) as resp, tmp_zip.open("wb") as fh:
        shutil.copyfileobj(resp, fh)

    if manifest.sha256:
        got = _sha256_file(tmp_zip)
        if got.lower() != manifest.sha256.lower():
            raise RuntimeError(f"Sai checksum update package. expected={manifest.sha256} got={got}")

    with tempfile.TemporaryDirectory(prefix="toolfb_update_extract_") as tdir:
        extract_root = Path(tdir)
        with zipfile.ZipFile(tmp_zip, "r") as zf:
            zf.extractall(extract_root)
        payload_root = _detect_update_payload_root(extract_root)

        backup_dir = updates_dir / f"backup_before_{manifest.version}"
        if backup_dir.exists():
            shutil.rmtree(backup_dir, ignore_errors=True)
        backup_dir.mkdir(parents=True, exist_ok=True)

        excludes = set(keep_dirs)
        for item in project_root.iterdir():
            if item.name in excludes:
                continue
            target = backup_dir / item.name
            if item.is_dir():
                shutil.copytree(item, target, dirs_exist_ok=True)
            else:
                shutil.copy2(item, target)

        # copy payload sang project root, bỏ qua data/venv/logs...
        for item in payload_root.iterdir():
            if item.name in excludes:
                continue
            target = project_root / item.name
            if item.is_dir():
                if target.exists():
                    shutil.rmtree(target, ignore_errors=True)
                shutil.copytree(item, target, dirs_exist_ok=True)
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(item, target)

        logger.info("Updater: áp dụng update {} thành công.", manifest.version)
        return backup_dir


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
