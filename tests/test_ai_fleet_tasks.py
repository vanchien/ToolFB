"""Tác vụ AI chỉ đổi trạng thái theo bảng, và mở lại app không biến running thành thành công."""

from __future__ import annotations

import pytest

from src.services.ai_fleet.store import FleetStore
from src.services.ai_fleet.tasks import can_retry, classify_error, enqueue, recover_running, redact, retry_task, transition


def test_topic_catalog_renames_and_deletes_across_pages(tmp_path) -> None:
    store = FleetStore(tmp_path / "fleet.json")
    store.upsert_profile({"page_id": "a", "topic": ["phim hài"]})
    store.upsert_profile({"page_id": "b", "topic": ["phim hài", "xe máy"]})
    store.save_topic_catalog(["phim hài", "xe máy"])
    store.rename_topic("phim hài", "phim cổ")
    assert store.load()["profiles"]["a"]["topic"] == ["phim cổ"]
    assert store.load()["profiles"]["b"]["topic"] == ["phim cổ", "xe máy"]
    store.delete_topic("xe máy")
    assert store.topic_catalog() == ["phim cổ"]
    store.save_hour_catalog(["08:00, 13:00", "08:00, 13:00", "21:30"])
    assert store.hour_catalog() == ["08:00, 13:00", "21:30"]
    store.delete_hour_frame("21:30")
    assert store.hour_catalog() == ["08:00, 13:00"]
    store.set_master_instruction("viết ngắn")
    store.set_master_instruction("viết ngắn")
    store.set_master_instruction("thêm lời chào")
    assert store.master_instruction() == "thêm lời chào"
    assert [row["text"] for row in store.load()["master_lessons"]] == ["viết ngắn", "thêm lời chào"]
    failed = enqueue(store, kind="write", page_id="a", detail="một")
    transition(store, failed["task_id"], "running")
    transition(store, failed["task_id"], "failed", note="lỗi")
    running = enqueue(store, kind="write", page_id="a", detail="hai")
    transition(store, running["task_id"], "running")
    assert store.delete_tasks([failed["task_id"], running["task_id"]]) == 1
    assert [row["task_id"] for row in store.tasks()] == [running["task_id"]]
    from src.ai.image_generation import chatgpt_profile_dir

    profile = chatgpt_profile_dir("../sk-secret")
    assert profile.parent.name == "chatgpt"
    assert profile.name == "sk-secret"
    assert ".." not in profile.parts
    assert chatgpt_profile_dir("").name == "default"
    store.mark_verified("openai", "key-1")
    store.forget_verified("openai", "key-1")
    assert store.verified_accounts() == []
    store.mark_verified("openai", "key-1")
    store.mark_verified("gemini", "key-2")
    store.set_selected_accounts(["key-1"], ["key-2"])
    assert store.verified_accounts() == [{"kind": "openai", "id": "key-1"}, {"kind": "gemini", "id": "key-2"}]
    assert store.selected_accounts()["gemini"] == ["key-2"]
    from src.services.ai_fleet.account_check import check_provider_key

    class Response:
        status_code = 200

    assert check_provider_key("openai", "sk-test", get=lambda *args, **kwargs: Response()) == ""
    assert "sk-test" not in check_provider_key("openai", "sk-test", get=lambda *args, **kwargs: type("R", (), {"status_code": 401})())
    from src.gui.ai_fleet_tab import _merge_hours

    assert _merge_hours("Tự rải trong ngày", "10:30") == "10:30"
    assert _merge_hours("08:00", "10:30") == "08:00, 10:30"
    assert _merge_hours("08:00, 10:30", "08:00, 19:00") == "08:00, 19:00"
    assert store.load()["profiles"]["b"]["topic"] == ["phim cổ"]


def test_illegal_transition_and_secret_redaction(tmp_path) -> None:
    store = FleetStore(tmp_path / "fleet.json")
    task = enqueue(store, kind="write", page_id="page-br", detail="phim")
    with pytest.raises(ValueError):
        transition(store, task["task_id"], "succeeded")
    assert store.tasks()[0]["state"] == "queued"
    assert "sk-live-secret" not in redact("lỗi sk-live-secret Bearer abc.def")


def test_restart_returns_running_task_to_queue(tmp_path) -> None:
    store = FleetStore(tmp_path / "fleet.json")
    task = enqueue(store, kind="write", page_id="page-br")
    transition(store, task["task_id"], "running", note="đang gọi")
    assert recover_running(store) == 0
    owned = store.tasks()[0]
    owned["owner_pid"] = 0
    store.save_task(owned)
    assert recover_running(store) == 1
    restored = store.tasks()[0]
    assert restored["state"] == "queued"
    assert restored["events"][-1]["note"] == "Khôi phục sau khi ứng dụng mở lại"
    assert restored["state"] != "succeeded"


def test_only_retryable_failures_can_be_queued_again(tmp_path) -> None:
    store = FleetStore(tmp_path / "fleet.json")
    task = enqueue(store, kind="write", page_id="page-br")
    transition(store, task["task_id"], "running")
    transition(store, task["task_id"], "failed", error_class=classify_error("HTTP 503"), error="HTTP 503")
    assert can_retry(store.tasks()[0])
    assert retry_task(store, task["task_id"])["state"] == "queued"
    other = enqueue(store, kind="write", page_id="page-br")
    transition(store, other["task_id"], "running")
    transition(store, other["task_id"], "failed", error_class="authentication", error="401")
    with pytest.raises(ValueError):
        retry_task(store, other["task_id"])
