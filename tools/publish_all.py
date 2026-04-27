from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

from github_release_urls import github_latest_asset_url
from publish_github_helpers import detect_github_repo, ensure_gh_authenticated


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _run(cmd: list[str], *, cwd: Path) -> None:
    subprocess.run(cmd, cwd=str(cwd), check=True)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "One-command release flow:\n"
            "1) bump/set version + build bundle + generate latest.json\n"
            "2) publish assets to GitHub Release (needs gh CLI; local: gh auth login; CI: GH_TOKEN)\n\n"
            "--repo defaults to git remote origin (github.com) or gh repo view.\n"
            "Writes config/update_channel.json with manifest_url unless --no-write-update-channel.\n\n"
            "If --download-url is omitted, the zip URL in latest.json defaults to:\n"
            "https://github.com/<repo>/releases/latest/download/ToolFB_release_bundle.zip\n"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--repo",
        default="",
        help="GitHub owner/name (e.g. your-org/ToolFB). Default: auto-detect from git or gh.",
    )
    parser.add_argument(
        "--download-url",
        default="",
        help=(
            "Public URL for ToolFB_release_bundle.zip (embedded in latest.json). "
            "Default when empty: https://github.com/<repo>/releases/latest/download/ToolFB_release_bundle.zip"
        ),
    )
    parser.add_argument("--notes", default="", help="Release notes for latest.json and GitHub release.")
    parser.add_argument("--bump", choices=("patch", "minor", "major"), default="patch", help="Semver bump level.")
    parser.add_argument("--version", default="", help="Explicit version (e.g. 1.4.0), overrides --bump.")
    parser.add_argument("--tag", default="", help="GitHub tag (default: v<version>).")
    parser.add_argument("--title", default="", help="GitHub release title (default: ToolFB <version>).")
    parser.add_argument("--draft", action="store_true", help="Create GitHub release as draft.")
    parser.add_argument("--prerelease", action="store_true", help="Mark GitHub release as prerelease.")
    parser.add_argument(
        "--no-write-update-channel",
        action="store_true",
        help="Do not write update channel JSON (e.g. GitHub Actions already uses the repo URL).",
    )
    parser.add_argument(
        "--update-channel-out",
        default="config/update_channel.json",
        help="Relative path for manifest_url JSON (ignored with --no-write-update-channel).",
    )
    args = parser.parse_args()

    root = _project_root()
    ensure_gh_authenticated(root)

    repo = str(args.repo).strip()
    if not repo:
        repo = detect_github_repo(root)
    if not repo:
        print(
            "ERROR: No GitHub repo. Pass --repo owner/name or set git remote origin to github.com.",
            file=sys.stderr,
        )
        return 1

    py = sys.executable

    dl = str(args.download_url).strip()
    if not dl:
        dl = github_latest_asset_url(repo, "ToolFB_release_bundle.zip")

    publish_manifest_cmd = [
        py,
        str(root / "tools" / "publish_release_manifest.py"),
        "--download-url",
        dl,
        "--bump",
        args.bump,
    ]
    if args.version.strip():
        publish_manifest_cmd.extend(["--version", args.version.strip()])
    if args.notes.strip():
        publish_manifest_cmd.extend(["--notes", args.notes.strip()])
    _run(publish_manifest_cmd, cwd=root)

    publish_gh_cmd = [
        py,
        str(root / "tools" / "publish_github_release.py"),
        "--repo",
        repo,
    ]
    if args.tag.strip():
        publish_gh_cmd.extend(["--tag", args.tag.strip()])
    if args.title.strip():
        publish_gh_cmd.extend(["--title", args.title.strip()])
    if args.notes.strip():
        publish_gh_cmd.extend(["--notes", args.notes.strip()])
    if args.draft:
        publish_gh_cmd.append("--draft")
    if args.prerelease:
        publish_gh_cmd.append("--prerelease")
    _run(publish_gh_cmd, cwd=root)

    manifest_url = github_latest_asset_url(repo, "latest.json")
    print(f"MANIFEST_URL={manifest_url}")
    print(f"DOWNLOAD_URL_IN_MANIFEST={dl}")

    if not args.no_write_update_channel:
        rel = str(args.update_channel_out).strip() or "config/update_channel.json"
        out_path = (root / rel).resolve() if not Path(rel).is_absolute() else Path(rel)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"manifest_url": manifest_url}
        out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(f"UPDATE_CHANNEL_WRITTEN={out_path}")

    print("PUBLISH_ALL_OK=1")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
