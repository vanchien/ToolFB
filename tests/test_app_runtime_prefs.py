"""Prefs runtime: nhớ lịch + quyết định tự bật sau reboot."""

from __future__ import annotations

from pathlib import Path

from src.utils import app_runtime_prefs as prefs


def test_runtime_prefs_roundtrip(tmp_path: Path) -> None:
    prefs.save_runtime_prefs(
        {"open_on_windows_startup": True, "scheduler_wanted": True},
        root=tmp_path,
    )
    data = prefs.load_runtime_prefs(root=tmp_path)
    assert data["open_on_windows_startup"] is True
    assert data["scheduler_wanted"] is True
    prefs.set_scheduler_wanted(False, root=tmp_path)
    assert prefs.load_runtime_prefs(root=tmp_path)["scheduler_wanted"] is False


def test_should_auto_start_when_wanted(monkeypatch) -> None:
    monkeypatch.setattr(prefs, "has_actionable_schedule_jobs", lambda: False)
    monkeypatch.setattr(prefs, "fleet_wants_scheduler", lambda: False)
    assert prefs.should_auto_start_scheduler({"scheduler_wanted": True}) is True
    assert prefs.should_auto_start_scheduler({"scheduler_wanted": False}) is False


def test_should_auto_start_when_unset_but_has_work(monkeypatch) -> None:
    monkeypatch.setattr(prefs, "has_actionable_schedule_jobs", lambda: True)
    monkeypatch.setattr(prefs, "fleet_wants_scheduler", lambda: False)
    assert prefs.should_auto_start_scheduler({"scheduler_wanted": None}) is True
    monkeypatch.setattr(prefs, "has_actionable_schedule_jobs", lambda: False)
    monkeypatch.setattr(prefs, "fleet_wants_scheduler", lambda: True)
    assert prefs.should_auto_start_scheduler({}) is True
    monkeypatch.setattr(prefs, "fleet_wants_scheduler", lambda: False)
    assert prefs.should_auto_start_scheduler({}) is False


def test_build_autostart_command_includes_gui(tmp_path: Path, monkeypatch) -> None:
    from src.utils import windows_autostart as wa

    (tmp_path / "main.py").write_text("# stub\n", encoding="utf-8")
    monkeypatch.setattr(wa.sys, "frozen", False, raising=False)
    monkeypatch.setattr(wa.sys, "executable", str(tmp_path / "python.exe"))
    cmd = wa.build_autostart_command(root=tmp_path)
    assert "--gui" in cmd
    assert "main.py" in cmd
