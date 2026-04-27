"""Smoke test lớp ``services`` + ``LibraryService.import_file``."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.services.account_service import AccountService
from src.services.library_service import LibraryService
from src.services.page_service import PageService
from src.services.scheduler_service import SchedulerService
from src.utils import page_workspace as pw
from src.utils.db_manager import AccountsDatabaseManager
from src.utils.pages_manager import PagesManager
from src.utils.schedule_posts_manager import SchedulePostsManager


def test_account_page_scheduler_services_empty_json(tmp_path: Path) -> None:
    acc_path = tmp_path / "accounts.json"
    acc_path.write_text("[]\n", encoding="utf-8")
    pages_path = tmp_path / "pages.json"
    pages_path.write_text("[]\n", encoding="utf-8")
    sch_path = tmp_path / "schedule_posts.json"
    sch_path.write_text("[]\n", encoding="utf-8")

    acc = AccountService(AccountsDatabaseManager(json_path=acc_path))
    assert acc.load_all() == []

    pages = PageService(PagesManager(json_path=pages_path))
    assert pages.load_all() == []

    sch = SchedulerService(SchedulePostsManager(json_path=sch_path))
    assert sch.load_all() == []


def test_library_service_import_text_image(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(pw, "project_root", lambda: tmp_path)
    pid = "p1librarytest"
    src_txt = tmp_path / "hello.txt"
    src_txt.write_text("nội dung", encoding="utf-8")
    src_img = tmp_path / "x.png"
    src_img.write_bytes(b"\x89PNG\r\n\x1a\n")

    lib = LibraryService()
    out_txt = lib.import_file(pid, src_txt, "text")
    out_img = lib.import_file(pid, src_img, "image")

    assert out_txt.name == "hello.txt"
    assert (out_txt.parent / f"{out_txt.stem}.import.meta.json").is_file()
    meta = (out_txt.parent / f"{out_txt.stem}.import.meta.json").read_text(encoding="utf-8")
    assert "source_path" in meta

    assert out_img.suffix == ".png"
    meta_img = (out_img.parent / f"{out_img.stem}.import.meta.json").read_text(encoding="utf-8")
    assert "size_bytes" in meta_img


def test_library_pick_respects_image_history(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(pw, "project_root", lambda: tmp_path)
    from src.services.post_history_service import PostHistoryService

    pid = "pickhistpage"
    pw.ensure_page_workspace(pid)
    lib = LibraryService()
    a = tmp_path / "a.png"
    b = tmp_path / "b.png"
    a.write_bytes(b"\x89PNG\r\n\x1a\n")
    b.write_bytes(b"\x89PNG\r\n\x1a\n")
    pa = lib.import_file(pid, a, "image")
    lib.import_file(pid, b, "image")
    hist = PostHistoryService(image_cooldown_days=14)
    hist.append_entry(pid, image_paths=[str(pa.resolve())])
    picked = lib.pick_random_eligible_image(pid, history=hist)
    assert picked is not None
    assert picked.resolve() != pa.resolve()


def test_library_service_rejects_bad_ext(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(pw, "project_root", lambda: tmp_path)
    pid = "p2badext"
    bad = tmp_path / "a.exe"
    bad.write_bytes(b"x")
    lib = LibraryService()
    with pytest.raises(ValueError, match="Định dạng"):
        lib.import_file(pid, bad, "text")
