from __future__ import annotations

from pathlib import Path

from src.utils.paths import project_root


def tiktok_data_root() -> Path:
    return project_root() / "data" / "tiktok"


def ensure_tiktok_layout() -> dict[str, Path]:
    root = tiktok_data_root()
    paths = {
        "root": root,
        "accounts": root / "accounts.json",
        "jobs": root / "upload_jobs.json",
        "logs": root / "logs",
        "screenshots": root / "logs" / "screenshots",
        "reports": root / "reports",
    }
    for key, p in paths.items():
        if key in ("accounts", "jobs"):
            p.parent.mkdir(parents=True, exist_ok=True)
            continue
        if p.suffix:
            p.parent.mkdir(parents=True, exist_ok=True)
        else:
            p.mkdir(parents=True, exist_ok=True)
    if not paths["accounts"].is_file():
        paths["accounts"].write_text("[]\n", encoding="utf-8")
    if not paths["jobs"].is_file():
        paths["jobs"].write_text("[]\n", encoding="utf-8")
    return paths
