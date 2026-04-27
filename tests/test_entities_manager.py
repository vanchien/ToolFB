"""Kiểm tra EntitiesManager (cache mtime, get_by_id)."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.utils.entities_manager import EntitiesManager, get_default_entities_manager


def _valid_row(eid: str = "e1", account_id: str = "a1") -> dict:
    return {
        "id": eid,
        "account_id": account_id,
        "name": "Test Page",
        "target_type": "fanpage",
        "target_url": "https://www.facebook.com/test",
        "schedule_time": "10:00",
    }


def test_load_all_uses_mtime_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Hai lần load_all liên tiếp không đọc file lần hai khi mtime không đổi."""
    path = tmp_path / "entities.json"
    path.write_text("[]", encoding="utf-8")
    mgr = EntitiesManager(json_path=path)
    reads: list[int] = []

    real_read = Path.read_text

    def wrapped_read_text(self: Path, *a: object, **k: object) -> str:
        if self.resolve() == path.resolve():
            reads.append(1)
        return real_read(self, *a, **k)  # type: ignore[misc]

    monkeypatch.setattr(Path, "read_text", wrapped_read_text)

    mgr.load_all()
    mgr.load_all()
    assert sum(reads) == 1


def test_save_all_refreshes_cache(tmp_path: Path) -> None:
    path = tmp_path / "entities.json"
    path.write_text("[]", encoding="utf-8")
    mgr = EntitiesManager(json_path=path)
    mgr.save_all([_valid_row()])
    got = mgr.get_by_id("e1")
    assert got is not None
    assert got["name"] == "Test Page"


def test_get_default_entities_manager_singleton() -> None:
    a = get_default_entities_manager()
    b = get_default_entities_manager()
    assert a is b
