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
    """
    version = _read_local_version(root)
    sha256 = _sha256_file(zip_path)
    root_url = os.environ.get("TOOLFB_RELEASE_DOWNLOAD_URL", "").strip()
    download_url = root_url or str(Path(zip_path).name)
    notes = os.environ.get("TOOLFB_RELEASE_NOTES", "").strip()
    payload = {
        "version": version,
        "download_url": download_url,
        "sha256": sha256,
        "notes": notes,
        "generated_at": datetime.now().replace(microsecond=0).isoformat(),
    }
    out = dist / "latest.json"
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return out


def build_release_bundle() -> tuple[Path, Path, Path]:
    root = _project_root()
    dist = root / "dist"
    dist.mkdir(parents=True, exist_ok=True)

    # 1) Build clean portable package
    _run([sys.executable, str(root / "tools" / "build_clean_portable.py")], cwd=root)

    # 2) Build GUI exe package
    _run([sys.executable, str(root / "tools" / "build_exe_gui.py")], cwd=root)

    # 3) Compose unified bundle
    bundle_dir = dist / "ToolFB_release_bundle"
    if bundle_dir.exists():
        shutil.rmtree(bundle_dir, ignore_errors=True)
    bundle_dir.mkdir(parents=True, exist_ok=True)

    clean_dir = dist / "ToolFB_portable_clean"
    exe_dir = dist / "ToolFB_GUI"

    shutil.copytree(clean_dir, bundle_dir / "portable_clean", dirs_exist_ok=True)
    shutil.copytree(exe_dir, bundle_dir / "exe_gui", dirs_exist_ok=True)

    readme = bundle_dir / "README_RELEASE.txt"
    readme.write_text(
        "ToolFB Release Bundle\n"
        "====================\n\n"
        "1) portable_clean/\n"
        "- Chua source + launcher Start_ToolFB_GUI.bat\n"
        "- Khong kem du lieu van hanh (accounts/pages/jobs/cookies/profiles/logs da reset)\n\n"
        "2) exe_gui/\n"
        "- Chay truc tiep ToolFB_GUI.exe khong can go lenh\n"
        "- Co Start_ToolFB_GUI.bat de mo nhanh\n"
        "- BAT BUOC: copy ca thu muc exe_gui (gom ToolFB_GUI.exe + _internal/...), khong chi copy file .exe le\n"
        "- Ban build day du da dong goi Chromium + Firefox + WebKit (Playwright) trong _internal/ms-playwright — may dich khong can `playwright install`\n"
        "- Kich thuoc zip lon (hang tram MB) do trinh duyet; build nhanh: dat TOOLFB_SKIP_BROWSER_BUNDLE=1 khi goi tools/build_exe_gui.py\n\n"
        "Goi y:\n"
        "- Neu may dich co Python/venv: dung portable_clean\n"
        "- Neu muon click-chay ngay tren may sach: dung exe_gui (build day du)\n",
        encoding="utf-8",
    )

    zip_base = dist / "ToolFB_release_bundle"
    zip_path = Path(shutil.make_archive(str(zip_base), "zip", root_dir=bundle_dir.parent, base_dir=bundle_dir.name))
    latest_path = _write_latest_manifest(root=root, zip_path=zip_path, dist=dist)
    return bundle_dir, zip_path, latest_path


if __name__ == "__main__":
    folder, archive, latest = build_release_bundle()
    print(f"RELEASE_BUNDLE_FOLDER={folder}")
    print(f"RELEASE_BUNDLE_ZIP={archive}")
    print(f"RELEASE_LATEST_JSON={latest}")
