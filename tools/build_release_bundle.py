from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _run(cmd: list[str], *, cwd: Path) -> None:
    subprocess.run(cmd, cwd=str(cwd), check=True)


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _prune_veo3studio_node_caches(veo_root: Path) -> int:
    """
    Xóa ``node_modules/.cache`` trong Veo3Studio trước khi đóng gói.

    Cache Prisma/npm tạo path cực dài, zip nặng và dễ lỗi khi giải nén trên Windows;
    Prisma có thể tải lại engine khi chạy server lần đầu.
    """
    removed = 0
    if not veo_root.is_dir():
        return 0
    for node_modules in veo_root.rglob("node_modules"):
        if not node_modules.is_dir():
            continue
        cache = node_modules / ".cache"
        if cache.is_dir():
            shutil.rmtree(cache, ignore_errors=True)
            removed += 1
    return removed


def _verify_release_browser_bundle(exe_dir: Path) -> None:
    """Release phân phối máy khách: bắt buộc có ms-playwright khớp manifest (không skip bundle)."""
    skip = os.environ.get("TOOLFB_SKIP_BROWSER_BUNDLE", "").strip().lower()
    if skip in {"1", "true", "yes", "on"}:
        raise RuntimeError(
            "TOOLFB_SKIP_BROWSER_BUNDLE=1 — bản zip KHÔNG dùng cho máy khách. "
            "Bỏ biến này rồi build lại để đóng gói Chromium/Firefox/WebKit."
        )
    bp = exe_dir / "_internal" / "ms-playwright"
    if not bp.is_dir() or not any(bp.iterdir()):
        raise RuntimeError(
            f"Thiếu {bp} sau build — máy khách sẽ lỗi «Executable doesn't exist». "
            "Chạy lại build (không đặt TOOLFB_SKIP_BROWSER_BUNDLE)."
        )
    root = _project_root()
    mf_path = root / "release" / "browser_bundle_manifest.json"
    expected: dict[str, str] = {}
    if mf_path.is_file():
        try:
            raw = json.loads(mf_path.read_text(encoding="utf-8"))
            if isinstance(raw, dict) and isinstance(raw.get("browsers"), dict):
                expected = {str(k): str(v) for k, v in raw["browsers"].items()}
        except Exception:
            pass
    for key, folder in (("firefox", expected.get("firefox", "firefox-1509")), ("chromium", expected.get("chromium", "chromium-1208"))):
        if not folder:
            continue
        if key == "firefox":
            exe = bp / folder / "firefox" / "firefox.exe"
        else:
            exe = bp / folder / "chrome-win64" / "chrome.exe"
            if not exe.is_file():
                exe = bp / folder / "chrome-win" / "chrome.exe"
        if not exe.is_file():
            raise RuntimeError(f"Thiếu {key} trong bundle: {exe}")
    vf = exe_dir / "version.json"
    if not vf.is_file():
        raise RuntimeError(
            f"Thiếu {vf} — máy mới sẽ hiện 0.0.0-dev và báo cập nhật giả. "
            "build_exe_gui phải copy version.json cạnh ToolFB_GUI.exe."
        )
    ffmpeg = exe_dir / "tools" / "ffmpeg" / "bin" / "ffmpeg.exe"
    if not ffmpeg.is_file():
        print(f"WARN: thiếu {ffmpeg} — Video Editor / schedule có thể lỗi ffmpeg.", file=sys.stderr)


def _read_local_version(root: Path) -> str:
    vf = root / "version.json"
    if vf.is_file():
        try:
            raw = json.loads(vf.read_text(encoding="utf-8"))
        except Exception:
            raw = {}
        if isinstance(raw, dict):
            v = str(raw.get("version", "")).strip()
            if v:
                return v
    return f"0.0.0-{datetime.now().strftime('%Y%m%d%H%M%S')}"


def _write_latest_manifest(*, root: Path, zip_path: Path, dist: Path) -> Path:
    """
    Tạo ``dist/latest.json`` dùng cho auto-updater.

    Env hỗ trợ:
    - ``TOOLFB_RELEASE_DOWNLOAD_URL``: URL zip cố định.
    - ``TOOLFB_RELEASE_NOTES``: ghi chú release.
    - ``TOOLFB_PATCH_DOWNLOAD_URL`` / ``TOOLFB_PATCH_SHA256``: ZIP vá (delta), máy client
      tải trước; updater merge từng file, chỉ ghi đè file có trong ZIP (xem ``build_delta_patch_zip.py``).
    """
    version = _read_local_version(root)
    sha256 = _sha256_file(zip_path)
    root_url = os.environ.get("TOOLFB_RELEASE_DOWNLOAD_URL", "").strip()
    download_url = root_url or str(Path(zip_path).name)
    notes = os.environ.get("TOOLFB_RELEASE_NOTES", "").strip()
    patch_url = os.environ.get("TOOLFB_PATCH_DOWNLOAD_URL", "").strip()
    patch_sha = os.environ.get("TOOLFB_PATCH_SHA256", "").strip().lower()
    payload: dict[str, str] = {
        "version": version,
        "download_url": download_url,
        "sha256": sha256,
        "notes": notes,
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
    }
    if patch_url:
        payload["patch_download_url"] = patch_url
        if patch_sha:
            payload["patch_sha256"] = patch_sha
    out = dist / "latest.json"
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return out


def build_release_bundle() -> tuple[Path, Path, Path]:
    root = _project_root()
    dist = root / "dist"
    dist.mkdir(parents=True, exist_ok=True)

    _ens_ff = root / "tools" / "ensure_ffmpeg_portable.py"
    if _ens_ff.is_file():
        subprocess.run([sys.executable, str(_ens_ff)], cwd=str(root), check=False)

    _ens = root / "tools" / "ensure_ytdlp_standalone_exe.py"
    if _ens.is_file():
        subprocess.run([sys.executable, str(_ens)], cwd=str(root), check=False)

    # 1) Build clean portable package
    _run([sys.executable, str(root / "tools" / "build_clean_portable.py")], cwd=root)

    # 2) Build GUI exe package
    _run([sys.executable, str(root / "tools" / "build_exe_gui.py")], cwd=root)
    _verify_release_browser_bundle(root / "dist" / "ToolFB_GUI")

    # 3) Compose unified bundle
    bundle_dir = dist / "ToolFB_release_bundle"
    if bundle_dir.exists():
        shutil.rmtree(bundle_dir, ignore_errors=True)
    bundle_dir.mkdir(parents=True, exist_ok=True)

    clean_dir = dist / "ToolFB_portable_clean"
    exe_dir = dist / "ToolFB_GUI"

    shutil.copytree(clean_dir, bundle_dir / "portable_clean", dirs_exist_ok=True)
    shutil.copytree(exe_dir, bundle_dir / "exe_gui", dirs_exist_ok=True)

    n1 = _prune_veo3studio_node_caches(bundle_dir / "portable_clean" / "tools" / "Veo3Studio")
    n2 = _prune_veo3studio_node_caches(bundle_dir / "exe_gui" / "tools" / "Veo3Studio")
    if n1 or n2:
        print(f"PRUNED_VEO3_NODE_CACHE_DIRS portable={n1} exe_gui={n2}", file=sys.stderr)

    # Launcher gốc — máy mới chỉ cần giải nén rồi double-click file này.
    root_launcher = bundle_dir / "Start_ToolFB.bat"
    root_launcher.write_text(
        "@echo off\r\n"
        "chcp 65001 >nul\r\n"
        "cd /d \"%~dp0\"\r\n"
        "if not exist \"exe_gui\\ToolFB_GUI.exe\" (\r\n"
        "  echo [LOI] Thieu exe_gui\\ToolFB_GUI.exe\r\n"
        "  echo Hay giai nen dung ToolFB_release_bundle.zip ^(giu nguyen cau truc thu muc^).\r\n"
        "  pause\r\n"
        "  exit /b 1\r\n"
        ")\r\n"
        "if not exist \"exe_gui\\_internal\\ms-playwright\\\" (\r\n"
        "  echo [LOI] Thieu exe_gui\\_internal\\ms-playwright\r\n"
        "  echo Ban zip khong day du hoac chi copy file .exe le.\r\n"
        "  echo Tai lai ToolFB_release_bundle.zip tu GitHub Releases ^(khoang 900MB+^).\r\n"
        "  pause\r\n"
        "  exit /b 1\r\n"
        ")\r\n"
        "cd /d \"%~dp0exe_gui\"\r\n"
        "start \"\" \"ToolFB_GUI.exe\" --gui\r\n",
        encoding="utf-8",
    )

    ver = _read_local_version(root)
    readme = bundle_dir / "README_RELEASE.txt"
    readme.write_text(
        f"ToolFB {ver} — Huong dan may moi\n"
        "================================\n\n"
        "CAI DAT NHANH (khuyen nghi):\n"
        "1) Giai nen TOAN BO file ToolFB_release_bundle.zip ra mot thu muc\n"
        "   (vi du: D:\\ToolFB) — KHONG chi copy ToolFB_GUI.exe le.\n"
        "2) Double-click Start_ToolFB.bat (o goc thu muc vua giai nen).\n"
        "3) App mo GUI — them tai khoan / Page trong bang dieu khien.\n\n"
        "Hoac mo: exe_gui\\Start_ToolFB_GUI.bat\n\n"
        "QUAN TRONG:\n"
        "- Can ca thu muc exe_gui (ToolFB_GUI.exe + _internal + config + tools).\n"
        "- Ban day du da kem Chromium/Firefox/WebKit trong _internal\\ms-playwright\n"
        "  → may dich KHONG can cai Python, KHONG can chay «playwright install».\n"
        "- Zip thieu ms-playwright (~nhe) se bao loi trinh duyet khi dang bai.\n"
        "- Nen giai nen ra o dia cuc bo (tranh OneDrive/path qua dai).\n\n"
        "Cac thu muc trong zip:\n"
        "1) Start_ToolFB.bat  ← bam de chay (may sach)\n"
        "2) exe_gui/          ← ban click-chay (co trinh duyet + ffmpeg + yt-dlp)\n"
        "3) portable_clean/   ← source Python (can scripts\\setup_windows.bat)\n"
        "   May moi KHONG dung portable_clean neu chua cai Python/.venv.\n\n"
        "Cap nhat sau nay: trong app bam «Cap nhat ngay» (giu config/data).\n",
        encoding="utf-8",
    )

    # Đảm bảo version.json có trong exe_gui (phòng build cũ thiếu bước copy).
    vf_src = root / "version.json"
    vf_dst = bundle_dir / "exe_gui" / "version.json"
    if vf_src.is_file():
        shutil.copy2(vf_src, vf_dst)

    zip_base = dist / "ToolFB_release_bundle"
    zip_path = Path(shutil.make_archive(str(zip_base), "zip", root_dir=bundle_dir.parent, base_dir=bundle_dir.name))
    latest_path = _write_latest_manifest(root=root, zip_path=zip_path, dist=dist)
    _sync_repo_latest_manifest(root=root, latest_path=latest_path)
    return bundle_dir, zip_path, latest_path


def _sync_repo_latest_manifest(*, root: Path, latest_path: Path) -> None:
    """
    Sao chép ``dist/latest.json`` → ``release/update/latest.json`` để push lên GitHub
    là máy client đọc raw manifest ngay sau build (không cần copy tay).
    """
    dest = root / "release" / "update" / "latest.json"
    try:
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(latest_path, dest)
        print(f"REPO_MANIFEST_SYNCED={dest}", flush=True)
    except OSError as exc:
        print(f"WARN: không copy manifest vào repo: {exc}", file=sys.stderr)


if __name__ == "__main__":
    folder, archive, latest = build_release_bundle()
    print(f"RELEASE_BUNDLE_FOLDER={folder}")
    print(f"RELEASE_BUNDLE_ZIP={archive}")
    print(f"RELEASE_LATEST_JSON={latest}")
