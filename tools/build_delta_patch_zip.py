from __future__ import annotations

"""
Sinh ZIP vá (delta) giữa hai cây ``portable_clean`` cùng cấu trúc với thư mục dự án.

Chỉ đóng gói các file **nội dung khác** (so SHA256) so với bản cũ — thư mục updater
``apply_update_package(..., is_delta=True)`` merge từng file, không xóa file không có
trong ZIP.

Workflow gợi ý:
1) Giữ bản ``portable_clean`` của release trước (giải nén từ zip cũ).
2) Build bản mới (``build_release_bundle``) → ``dist/ToolFB_release_bundle/portable_clean``.
3) Chạy script này → ``dist/ToolFB_delta_patch.zip`` + in ra SHA256.
4) Đăng kèm asset patch trên GitHub Release, truyền ``--patch-download-url`` và
   ``--patch-sha256`` cho ``publish_release_manifest.py``.

Ví dụ:

  python tools/build_delta_patch_zip.py ^
    --old "D:/ToolFB_1.0.45/portable_clean" ^
    --new "dist/ToolFB_release_bundle/portable_clean" ^
    --out "dist/ToolFB_delta_1.0.46.zip"
"""

import argparse
import hashlib
import shutil
import sys
import tempfile
import zipfile
from pathlib import Path


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _sha256_file(path: Path, *, chunk: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            b = fh.read(chunk)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def _collect_rel_sha256(root: Path) -> dict[str, str]:
    root = root.resolve()
    out: dict[str, str] = {}
    for p in root.rglob("*"):
        if not p.is_file():
            continue
        rel = p.relative_to(root)
        if ".." in rel.parts:
            continue
        sp = rel.as_posix()
        if "/__pycache__/" in f"/{sp}/" or sp.startswith("__pycache__/") or sp.endswith(".pyc"):
            continue
        out[sp] = _sha256_file(p)
    return out


def _find_portable_clean_under_extract(td: Path) -> Path:
    bundle_pc = td / "ToolFB_release_bundle" / "portable_clean"
    flat_pc = td / "portable_clean"
    if (bundle_pc / "src").is_dir() and (bundle_pc / "main.py").is_file():
        return bundle_pc
    if (flat_pc / "src").is_dir() and (flat_pc / "main.py").is_file():
        return flat_pc
    for c in td.iterdir():
        if c.is_dir() and (c / "main.py").is_file() and (c / "src").is_dir():
            return c
    raise RuntimeError(
        "Không tìm thấy portable_clean hợp lệ trong ZIP cũ "
        "(cần ToolFB_release_bundle/portable_clean hoặc portable_clean có main.py + src)."
    )


def _extract_old_portable_from_release_zip(zip_path: Path) -> tuple[Path, Path]:
    """Trả về ``(portable_clean, thư_mục_tạm_cần_xóa)``."""
    td = Path(tempfile.mkdtemp(prefix="toolfb_old_pkg_"))
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(td)
    return _find_portable_clean_under_extract(td), td


def _build_patch(
    old_root: Path,
    new_root: Path,
    out_zip: Path,
) -> tuple[int, int]:
    """
    Returns:
        (số file trong ZIP vá, tổng số file trên bản mới)
    """
    old_h = _collect_rel_sha256(old_root)
    new_h = _collect_rel_sha256(new_root)
    changed: list[str] = []
    for rel, nh in new_h.items():
        if rel not in old_h or old_h[rel] != nh:
            changed.append(rel)
    changed.sort()
    out_zip.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(out_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for rel in changed:
            src = new_root / rel
            if not src.is_file():
                continue
            zf.write(src, rel)
    return len(changed), len(new_h)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument(
        "--old",
        default="",
        help="Thư mục portable_clean của bản cũ (khuyên dùng).",
    )
    ap.add_argument(
        "--old-release-zip",
        default="",
        help="ZIP release đầy đủ bản trước; script tự tìm portable_clean bên trong.",
    )
    ap.add_argument("--new", required=True, help="Thư mục portable_clean bản mới (sau build).")
    ap.add_argument(
        "--out",
        default="dist/ToolFB_delta_patch.zip",
        help="Đường dẫn file ZIP vá ghi ra.",
    )
    args = ap.parse_args()

    old_s = str(args.old).strip()
    old_z = str(args.old_release_zip).strip()
    if bool(old_s) == bool(old_z):
        print("ERROR: Cần đúng một trong hai: --old HOẶC --old-release-zip.", file=sys.stderr)
        return 2
    new_root = Path(args.new).resolve()
    if not new_root.is_dir() or not (new_root / "main.py").is_file() or not (new_root / "src").is_dir():
        print(f"ERROR: --new không phải portable_clean hợp lệ: {new_root}", file=sys.stderr)
        return 2

    td_extract: Path | None = None
    try:
        if old_s:
            old_root = Path(old_s).resolve()
        else:
            zp = Path(old_z).resolve()
            if not zp.is_file():
                print(f"ERROR: không tìm thấy --old-release-zip: {zp}", file=sys.stderr)
                return 2
            old_root, td_extract = _extract_old_portable_from_release_zip(zp)
        if not old_root.is_dir():
            print(f"ERROR: thư mục bản cũ không tồn tại: {old_root}", file=sys.stderr)
            return 2

        out_zip = Path(args.out)
        if not out_zip.is_absolute():
            out_zip = (_project_root() / out_zip).resolve()
        n_patch, n_new = _build_patch(old_root, new_root, out_zip)
        sha = _sha256_file(out_zip)
        print(f"DELTA_PATCH_ZIP={out_zip}")
        print(f"DELTA_PATCH_FILES={n_patch} / NEW_TOTAL_FILES={n_new}")
        print(f"DELTA_PATCH_SHA256={sha}")
        return 0
    finally:
        if td_extract is not None:
            shutil.rmtree(td_extract, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
