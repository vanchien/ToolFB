from __future__ import annotations

import argparse
import shutil
from datetime import datetime
from pathlib import Path


CONFIG_FILES: tuple[str, ...] = (
    "accounts.json",
    "pages.json",
    "schedule_posts.json",
    "entities.json",
    "schedule.json",
    "app_secrets.json",
)

DATA_DIRS: tuple[str, ...] = (
    "cookies",
    "profiles",
    "nanobanana",
)


def _copy_file(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def _backup_target_items(target_root: Path, *, stamp: str) -> Path:
    backup_root = target_root / "data" / "runtime" / f"migration_backup_{stamp}"
    backup_root.mkdir(parents=True, exist_ok=True)
    for name in CONFIG_FILES:
        p = target_root / "config" / name
        if p.is_file():
            _copy_file(p, backup_root / "config" / name)
    for d in DATA_DIRS:
        p = target_root / "data" / d
        if p.is_dir():
            shutil.copytree(p, backup_root / "data" / d, dirs_exist_ok=True)
    return backup_root


def migrate_user_data(*, source_root: Path, target_root: Path, backup: bool = True) -> tuple[list[str], Path | None]:
    copied: list[str] = []
    backup_path: Path | None = None
    if backup:
        backup_path = _backup_target_items(target_root, stamp=datetime.now().strftime("%Y%m%d_%H%M%S"))

    for name in CONFIG_FILES:
        src = source_root / "config" / name
        dst = target_root / "config" / name
        if src.is_file():
            _copy_file(src, dst)
            copied.append(f"config/{name}")

    for d in DATA_DIRS:
        src = source_root / "data" / d
        dst = target_root / "data" / d
        if src.is_dir():
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copytree(src, dst, dirs_exist_ok=True)
            copied.append(f"data/{d}/")

    return copied, backup_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Migrate user data (accounts/pages/jobs/cookies/profiles/api keys) from old ToolFB folder to new one."
    )
    parser.add_argument("--from", dest="source", required=True, help="Old ToolFB root folder")
    parser.add_argument("--to", dest="target", required=True, help="New ToolFB root folder")
    parser.add_argument("--no-backup", action="store_true", help="Skip automatic backup in target data/runtime/")
    args = parser.parse_args()

    source_root = Path(args.source).resolve()
    target_root = Path(args.target).resolve()
    if not source_root.is_dir():
        raise SystemExit(f"Nguon khong ton tai: {source_root}")
    if not target_root.is_dir():
        raise SystemExit(f"Dich khong ton tai: {target_root}")

    copied, backup_path = migrate_user_data(
        source_root=source_root,
        target_root=target_root,
        backup=not args.no_backup,
    )
    print(f"MIGRATE_SOURCE={source_root}")
    print(f"MIGRATE_TARGET={target_root}")
    if backup_path is not None:
        print(f"MIGRATE_BACKUP={backup_path}")
    for item in copied:
        print(f"MIGRATED={item}")
    print(f"MIGRATED_COUNT={len(copied)}")


if __name__ == "__main__":
    main()
