"""Bootstrap config sau clone GitHub."""

from __future__ import annotations

import json
from pathlib import Path

from src.utils.first_run_bootstrap import bootstrap_all, setup_status


def test_bootstrap_creates_config_from_examples(tmp_path: Path) -> None:
    cfg = tmp_path / "config"
    cfg.mkdir()
    (cfg / "accounts.example.json").write_text("[]\n", encoding="utf-8")
    (cfg / "pages.example.json").write_text("[]\n", encoding="utf-8")
    (cfg / "schedule_posts.example.json").write_text("[]\n", encoding="utf-8")
    (cfg / "app_secrets.example.json").write_text("{}\n", encoding="utf-8")

    info = bootstrap_all(root=tmp_path)
    assert "accounts.json" in info["created_config"]
    assert (cfg / "accounts.json").is_file()
    assert (tmp_path / "data" / "cookies").is_dir()


def test_setup_status_needs_account(tmp_path: Path) -> None:
    cfg = tmp_path / "config"
    cfg.mkdir()
    (cfg / "accounts.json").write_text("[]\n", encoding="utf-8")
    st = setup_status(root=tmp_path)
    assert st["needs_account"] is True
    assert st["ready"] is False


def test_setup_status_ready(tmp_path: Path) -> None:
    cfg = tmp_path / "config"
    cfg.mkdir()
    acc = [{"id": "a1", "name": "n", "browser_type": "firefox", "portable_path": "p", "profile_path": "p", "cookie_path": "c", "proxy": {"host": "", "port": 0, "user": "", "pass": ""}, "use_proxy": False}]
    (cfg / "accounts.json").write_text(json.dumps(acc), encoding="utf-8")
    (cfg / "app_secrets.json").write_text('{"gemini_keys":[]}', encoding="utf-8")
    st = setup_status(root=tmp_path)
    assert st["n_accounts"] == 1
    assert st["ready"] is True
