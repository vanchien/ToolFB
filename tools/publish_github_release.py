from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

from github_release_urls import github_latest_asset_url
from publish_github_helpers import gh_cli


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _run(cmd: list[str], *, cwd: Path) -> str:
    p = subprocess.run(cmd, cwd=str(cwd), check=True, capture_output=True, text=True)
    return (p.stdout or "").strip()


def _ensure_file(path: Path) -> None:
    if not path.is_file():
        raise FileNotFoundError(str(path))


def _read_version(dist_dir: Path) -> str:
    latest = dist_dir / "latest.json"
    _ensure_file(latest)
    raw = json.loads(latest.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("dist/latest.json không hợp lệ.")
    version = str(raw.get("version", "")).strip()
    if not version:
        raise ValueError("dist/latest.json thiếu version.")
    return version


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Publish ToolFB release assets to GitHub Releases via gh CLI."
    )
    parser.add_argument(
        "--repo",
        required=True,
        help="GitHub repo in owner/name format (e.g. your-org/ToolFB).",
    )
    parser.add_argument(
        "--tag",
        default="",
        help="Release tag (default: v<version from dist/latest.json>).",
    )
    parser.add_argument(
        "--title",
        default="",
        help="Release title (default: ToolFB <version>).",
    )
    parser.add_argument(
        "--notes",
        default="",
        help="Release notes text (default from dist/latest.json notes).",
    )
    parser.add_argument(
        "--draft",
        action="store_true",
        help="Create as draft release.",
    )
    parser.add_argument(
        "--prerelease",
        action="store_true",
        help="Mark as prerelease.",
    )
    args = parser.parse_args()

    root = _project_root()
    dist = root / "dist"
    zip_path = dist / "ToolFB_release_bundle.zip"
    latest_json = dist / "latest.json"
    _ensure_file(zip_path)
    _ensure_file(latest_json)

    latest = json.loads(latest_json.read_text(encoding="utf-8"))
    if not isinstance(latest, dict):
        raise ValueError("dist/latest.json không hợp lệ.")
    version = _read_version(dist)
    tag = args.tag.strip() or f"v{version}"
    title = args.title.strip() or f"ToolFB {version}"
    notes = args.notes.strip() or str(latest.get("notes", "")).strip() or f"Release {version}"

    # Ensure gh auth exists and repo reachable.
    _run(gh_cli("auth", "status"), cwd=root)
    _run(gh_cli("repo", "view", args.repo), cwd=root)

    cmd = gh_cli(
        "release",
        "create",
        tag,
        str(zip_path),
        str(latest_json),
        "--repo",
        args.repo,
        "--title",
        title,
        "--notes",
        notes,
    )
    if args.draft:
        cmd.append("--draft")
    if args.prerelease:
        cmd.append("--prerelease")

    out = _run(cmd, cwd=root)
    print(f"RELEASE_TAG={tag}")
    print(f"RELEASE_REPO={args.repo}")
    print(f"MANIFEST_URL={github_latest_asset_url(args.repo, 'latest.json')}")
    print(f"DOWNLOAD_URL_HINT={github_latest_asset_url(args.repo, 'ToolFB_release_bundle.zip')}")
    if out:
        print(out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
