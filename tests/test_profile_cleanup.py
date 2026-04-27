"""Dọn thư mục profile dưới data/profiles không còn trong accounts."""

from __future__ import annotations

from pathlib import Path

from src.utils.profile_cleanup import (
    cleanup_orphan_profile_directories,
    collect_referenced_profile_paths,
    iter_profile_leaf_dirs,
    profiles_data_dir,
)


def _minimal_account(portable: str, *, cookie_path: str = "data/cookies/x.json") -> dict:
    return {
        "id": "acc1",
        "name": "n",
        "browser_type": "chromium",
        "portable_path": portable,
        "proxy": {"host": "", "port": 0, "user": "", "pass": ""},
        "cookie_path": cookie_path,
    }


def test_cleanup_deletes_only_orphan(tmp_path: Path) -> None:
    root = tmp_path / "proj"
    prof = root / "data" / "profiles"
    ck = root / "data" / "cookies"
    keep = prof / "firefox" / "acc_keep"
    orphan = prof / "firefox" / "acc_orphan"
    legacy = prof / "legacy_only"
    keep.mkdir(parents=True)
    orphan.mkdir(parents=True)
    legacy.mkdir(parents=True)
    ck.mkdir(parents=True)
    (ck / "acc_keep.json").write_text("[]\n", encoding="utf-8")
    (ck / "acc_orphan.json").write_text("[]\n", encoding="utf-8")
    (ck / "legacy_only.json").write_text("[]\n", encoding="utf-8")

    acc = _minimal_account(
        "data/profiles/firefox/acc_keep",
        cookie_path="data/cookies/acc_keep.json",
    )
    deleted = cleanup_orphan_profile_directories([acc], project_root=root, dry_run=False)
    assert keep.is_dir()
    assert not orphan.is_dir()
    assert not legacy.is_dir()
    assert (ck / "acc_keep.json").is_file()
    assert not (ck / "acc_orphan.json").is_file()
    assert not (ck / "legacy_only.json").is_file()
    assert len(deleted) == 4


def test_cleanup_skips_when_no_accounts(tmp_path: Path) -> None:
    prof = tmp_path / "data" / "profiles" / "firefox" / "x"
    prof.mkdir(parents=True)
    deleted = cleanup_orphan_profile_directories([], project_root=tmp_path, dry_run=False)
    assert prof.is_dir()
    assert deleted == []


def test_collect_referenced_both_keys(tmp_path: Path) -> None:
    root = tmp_path
    d1 = root / "data" / "profiles" / "chromium" / "a"
    d2 = root / "data" / "profiles" / "chromium" / "b"
    d1.mkdir(parents=True)
    d2.mkdir(parents=True)
    acc = {
        "portable_path": str(d1),
        "profile_path": "data/profiles/chromium/b",
    }
    refs = collect_referenced_profile_paths(root, [acc])
    assert d1.resolve() in refs
    assert d2.resolve() in refs


def test_iter_profile_leaf_dirs(tmp_path: Path) -> None:
    pr = profiles_data_dir(tmp_path)
    (pr / "firefox" / "p1").mkdir(parents=True)
    (pr / "loose").mkdir(parents=True)
    leaves = {p.name for p in iter_profile_leaf_dirs(pr)}
    assert leaves == {"p1", "loose"}
