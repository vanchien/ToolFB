"""Workspace Page: thư mục library + page_ai_config.json."""

from __future__ import annotations

from pathlib import Path

from src.utils import page_workspace as pw


def test_ensure_page_workspace_and_ai_config(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(pw, "project_root", lambda: tmp_path)
    pid = "abc12pageid9"
    root = pw.ensure_page_workspace(pid)
    assert root == tmp_path / "data" / "pages" / pid
    assert (root / "library" / "images").is_dir()
    assert (root / "library" / "texts").is_dir()
    assert (root / "page_ai_config.json").is_file()

    cfg = pw.load_page_ai_config(pid)
    assert cfg["page_id"] == pid
    assert isinstance(cfg.get("hashtags"), list)

    pw.save_page_ai_config(
        pid,
        {
            "brand_voice": "Thân thiện",
            "hashtags": ["#x", "#y"],
            "auto_generate_image": True,
        },
    )
    cfg2 = pw.load_page_ai_config(pid)
    assert cfg2["brand_voice"] == "Thân thiện"
    assert cfg2["hashtags"] == ["#x", "#y"]
    assert cfg2["auto_generate_image"] is True
    assert cfg2.get("post_length") == "medium"


def test_sanitize_rejects_unsafe() -> None:
    try:
        pw.sanitize_page_id("../x")
    except ValueError:
        return
    raise AssertionError("expected ValueError")
