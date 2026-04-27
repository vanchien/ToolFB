from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from pathlib import Path

from build_release_bundle import build_release_bundle


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _parse_semver(v: str) -> tuple[int, int, int] | None:
    s = str(v or "").strip()
    if not s:
        return None
    core = s.split("-", 1)[0]
    parts = core.split(".")
    if len(parts) < 3:
        return None
    try:
        return int(parts[0]), int(parts[1]), int(parts[2])
    except ValueError:
        return None


def _bump_version(current: str, level: str) -> str:
    parsed = _parse_semver(current)
    if parsed is None:
        parsed = (0, 0, 0)
    major, minor, patch = parsed
    lv = (level or "patch").strip().lower()
    if lv == "major":
        major, minor, patch = major + 1, 0, 0
    elif lv == "minor":
        major, minor, patch = major, minor + 1, 0
    else:
        major, minor, patch = major, minor, patch + 1
    return f"{major}.{minor}.{patch}"


def _read_version_file(path: Path) -> dict[str, str]:
    if not path.is_file():
        return {"version": "0.0.0", "channel": "stable"}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"version": "0.0.0", "channel": "stable"}
    if not isinstance(raw, dict):
        return {"version": "0.0.0", "channel": "stable"}
    out = dict(raw)
    out["version"] = str(out.get("version", "0.0.0")).strip() or "0.0.0"
    out["channel"] = str(out.get("channel", "stable")).strip() or "stable"
    return {"version": out["version"], "channel": out["channel"]}


def _write_version_file(path: Path, payload: dict[str, str]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Bump version + build release bundle + generate dist/latest.json.\n"
            "Example:\n"
            "  python tools/publish_release_manifest.py --download-url "
            "https://github.com/OWNER/REPO/releases/latest/download/ToolFB_release_bundle.zip "
            "--notes \"Fix profile + updater\""
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--download-url", default="", help="Public URL for ToolFB_release_bundle.zip")
    parser.add_argument("--notes", default="", help="Short release notes for latest.json")
    parser.add_argument("--bump", choices=("patch", "minor", "major"), default="patch", help="Semver bump level")
    parser.add_argument("--version", default="", help="Set explicit version (e.g. 1.4.0), overrides --bump")
    args = parser.parse_args()

    root = _project_root()
    version_file = root / "version.json"
    current = _read_version_file(version_file)
    next_version = str(args.version).strip() or _bump_version(current["version"], args.bump)
    payload = {
        "version": next_version,
        "channel": current.get("channel", "stable"),
        "updated_at": datetime.now().replace(microsecond=0).isoformat(),
    }
    _write_version_file(version_file, payload)
    print(f"VERSION_OLD={current['version']}")
    print(f"VERSION_NEW={next_version}")

    if args.download_url.strip():
        os.environ["TOOLFB_RELEASE_DOWNLOAD_URL"] = args.download_url.strip()
    if args.notes.strip():
        os.environ["TOOLFB_RELEASE_NOTES"] = args.notes.strip()

    folder, archive, latest = build_release_bundle()
    print(f"RELEASE_BUNDLE_FOLDER={folder}")
    print(f"RELEASE_BUNDLE_ZIP={archive}")
    print(f"RELEASE_LATEST_JSON={latest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
