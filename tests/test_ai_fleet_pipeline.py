"""Nhà viết bài và nhà tạo ảnh tách nhau. Bài trùng hoặc chưa duyệt không vào lịch."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.services.ai_fleet.gate import schedule_block_reason
from src.services.ai_fleet.pipeline import create_batch, create_post, delete_post, note_review, page_report, text_provider
from src.services.ai_fleet.store import FleetStore


def _allow_image(monkeypatch, tmp_path: Path) -> None:
    """Ảnh giả để vòng tự động duyệt được bài kèm ảnh, không gọi API thật."""

    class Image:
        def generate_images(self, *, prompt: str, number_of_images: int, model: str | None = None) -> list[bytes]:
            return [b"png"]

    monkeypatch.setattr("src.services.ai_fleet.pipeline.AIProviderFactory.image", lambda name: Image())

    def save(data: bytes, original_name: str) -> Path:
        path = tmp_path / Path(original_name).name
        path.write_bytes(data)
        return path

    monkeypatch.setattr("src.services.ai_fleet.pipeline.save_upload_to_library", save)


def _ready(title: str = "Phim hài cổ") -> dict:
    return {
        "title": title,
        "body": "Một bộ phim hài cổ điển đang được nhắc lại.",
        "caption": "Xem lại phim hài cổ điển.",
        "hashtags": ["#comedia"],
        "image_prompt": "poster phim hài cổ",
        "language": "pt-BR",
        "provider": "openai",
        "model": "gpt-test",
        "usage": {"input_tokens": 4, "output_tokens": 10},
    }


class _Text:
    provider_name = "openai"
    calls = 0

    def generate_content_package(self, *, prompt: str, model: str | None = None) -> dict:
        _Text.calls += 1
        return _ready(f"Phim { _Text.calls }")


class _SameText(_Text):
    def generate_content_package(self, *, prompt: str, model: str | None = None) -> dict:
        _Text.calls += 1
        return _ready("Phim hài cổ")


class _Image:
    provider_name = "gemini"
    calls: list[str] = []

    def generate_images(self, *, prompt: str, number_of_images: int, model: str | None = None) -> list[bytes]:
        self.calls.append(self.provider_name)
        return [b"png-bytes"]


def _store(tmp_path: Path) -> FleetStore:
    store = FleetStore(tmp_path / "fleet.json")
    store.upsert_profile({"page_id": "page-br", "page_name": "Brazil", "language": "pt-BR"})
    store.add_policy("page-br", {"language": "pt-BR", "requires_source": False, "requires_media": True}, requested_by="user")
    return store


def test_text_and_image_providers_stay_independent(tmp_path, monkeypatch) -> None:
    store = _store(tmp_path)
    image = _Image()
    monkeypatch.setattr("src.services.ai_fleet.pipeline.AIProviderFactory.text", lambda name: _Text())
    monkeypatch.setattr(
        "src.services.ai_fleet.pipeline.AIProviderFactory.image",
        lambda name: image if name == "gemini" else (_ for _ in ()).throw(AssertionError(name)),
    )

    def save(data: bytes, original_name: str) -> Path:
        path = tmp_path / Path(original_name).name
        path.write_bytes(data)
        return path

    monkeypatch.setattr("src.services.ai_fleet.pipeline.save_upload_to_library", save)
    result = create_post(store, page_id="page-br", topic="phim", text_name="openai", image_name="gemini", make_image=True)
    content = result["content"]
    version = content["versions"][0]
    assert content["provider"] == "openai"
    assert version["image_provider"] == "gemini"
    assert version["asset_ids"] == [result["asset_id"]]
    assert image.calls == ["gemini"]
    assert schedule_block_reason(content["content_id"], store)
    assert store.provider_choices()["text"] == "openai"


def test_duplicate_and_budget_do_not_keep_creating(tmp_path, monkeypatch) -> None:
    store = _store(tmp_path)
    _Text.calls = 0
    monkeypatch.setattr("src.services.ai_fleet.pipeline.AIProviderFactory.text", lambda name: _SameText())
    first = create_batch(store, page_id="page-br", topics=["một", "một", "hai"], text_name="openai", image_name="openai", max_output_tokens=10)
    assert first["items"][1]["skipped"] == "duplicate"
    assert first["items"][2]["skipped"] == "budget"
    assert _Text.calls == 1
    again = create_batch(store, page_id="page-br", topics=["ba"], text_name="openai", image_name="openai")
    assert again["items"][0]["skipped"] == "duplicate"
    assert len(store.load()["contents"]) == 1


def test_chatgpt_image_without_credit_uses_gemini(tmp_path, monkeypatch) -> None:
    store = _store(tmp_path)
    store.add_policy("page-br", {"language": "pt-BR", "requires_source": False, "requires_media": False}, requested_by="user")
    monkeypatch.setattr("src.services.ai_fleet.pipeline.AIProviderFactory.text", lambda name: _Text())

    class OpenAIImage:
        def generate_images(self, *, prompt: str, number_of_images: int, model: str | None = None) -> list[bytes]:
            raise RuntimeError("Tài khoản ChatGPT đã hết credit.")

    class GeminiImage:
        def generate_images(self, *, prompt: str, number_of_images: int, model: str | None = None) -> list[bytes]:
            return [b"png-bytes"]

    monkeypatch.setattr(
        "src.services.ai_fleet.pipeline.AIProviderFactory.image",
        lambda name: OpenAIImage() if name == "openai" else GeminiImage(),
    )
    result = create_post(store, page_id="page-br", topic="phim", text_name="openai", image_name="openai", make_image=True)
    assert "hết credit" in result["image_error"]
    assert not result["asset_id"]
    assert result["content"] is None
    assert store.load()["contents"] == {}


def test_image_failure_does_not_switch_provider(tmp_path, monkeypatch) -> None:
    store = _store(tmp_path)
    monkeypatch.setattr("src.services.ai_fleet.pipeline.AIProviderFactory.text", lambda name: _Text())

    def image(name: str):
        assert name == "gemini"

        class Broken:
            provider_name = "gemini"

            def generate_images(self, *, prompt: str, number_of_images: int, model: str | None = None) -> list[bytes]:
                raise RuntimeError("sk-secret leaked")

        return Broken()

    monkeypatch.setattr("src.services.ai_fleet.pipeline.AIProviderFactory.image", image)
    result = create_batch(
        store,
        page_id="page-br",
        topics=["một"],
        text_name="openai",
        image_name="gemini",
        make_image=True,
    )
    item = result["items"][0]
    assert item["content"] is None
    assert "sk-secret" not in item.get("image_error", "")
    assert "gemini" in item.get("image_error", "")
    assert store.load()["contents"] == {}


def test_resume_reuses_the_draft_already_saved(tmp_path, monkeypatch) -> None:
    store = _store(tmp_path)
    content = store.create_draft("page-br", _ready("Giữ bản này"))
    task = {
        "task_id": "task_resume",
        "state": "queued",
        "page_id": "page-br",
        "detail": "phim",
        "text_name": "openai",
        "image_name": "gemini",
        "make_image": False,
        "content_id": content["content_id"],
        "events": [],
    }
    store.save_task(task)

    def refuse(name: str):
        raise AssertionError(name)

    monkeypatch.setattr("src.services.ai_fleet.pipeline.AIProviderFactory.text", refuse)
    from src.services.ai_fleet.pipeline import resume_task

    result = resume_task(store, "task_resume")
    assert result["content"]["content_id"] == content["content_id"]
    assert len(store.load()["contents"]) == 1
    assert store.tasks()[0]["state"] == "waiting_approval"


def test_automatic_run_schedules_only_when_checks_pass(tmp_path, monkeypatch) -> None:
    store = _store(tmp_path)
    store.upsert_profile({"page_id": "page-br", "page_name": "Brazil", "language": "pt-BR", "topic": ["phim"], "status": "ACTIVE"})
    store.add_policy("page-br", {"language": "pt-BR", "requires_source": False, "requires_media": False, "min_score": 80}, requested_by="user")
    monkeypatch.setattr("src.services.ai_fleet.pipeline.AIProviderFactory.text", lambda name: _SameText())
    posts_path = tmp_path / "jobs.json"

    class Posts:
        def __init__(self) -> None:
            self.jobs: list[dict] = []

        def upsert(self, row: dict) -> None:
            self.jobs.append(dict(row))

    posts = Posts()
    from src.services.ai_fleet.pipeline import run_automatic

    rows = run_automatic(
        store,
        pages=[{"page_id": "page-br", "account_id": "acc-1", "page_name": "Brazil"}],
        text_name="openai",
        image_name="gemini",
        make_image=False,
        posts=posts,
    )
    assert rows[0]["result"] == "đã vào lịch"
    assert posts.jobs[0]["fleet_content_id"]
    assert posts.jobs[0]["content"] == "Xem lại phim hài cổ điển."
    assert posts_path.exists() is False


def test_automatic_run_keeps_going_when_one_page_fails_and_stamps_language(tmp_path, monkeypatch) -> None:
    store = _store(tmp_path)
    store.upsert_profile({"page_id": "page-br", "language": "pt-BR", "topic": ["một", "hai"], "status": "ACTIVE", "daily_content_target": 2})
    store.add_policy("page-br", {"language": "pt-BR", "requires_source": False, "requires_media": False}, requested_by="user")

    class Mixed(_SameText):
        def generate_content_package(self, *, prompt: str, model: str | None = None) -> dict:
            _Text.calls += 1
            if _Text.calls == 1:
                row = _ready("Bài một")
                row["language"] = "Portuguese"
                return row
            raise RuntimeError("OpenAI text lỗi HTTP 503")

    _Text.calls = 0
    monkeypatch.setattr("src.services.ai_fleet.pipeline.AIProviderFactory.text", lambda name: Mixed())

    class Posts:
        def __init__(self) -> None:
            self.jobs: list[dict] = []

        def upsert(self, row: dict) -> None:
            self.jobs.append(dict(row))

    from src.services.ai_fleet.gate import publish_block_reason
    from src.services.ai_fleet.pipeline import run_automatic

    posts = Posts()
    rows = run_automatic(
        store,
        pages=[{"page_id": "page-br", "account_id": "acc-1", "page_name": "Brazil"}],
        text_name="openai",
        image_name="openai",
        make_image=False,
        posts=posts,
    )
    assert rows[0]["result"] == "đã vào lịch"
    assert "503" in rows[1]["result"]
    content_id = posts.jobs[0]["fleet_content_id"]
    assert store.current_version(content_id)["language"] == "pt-BR"
    assert publish_block_reason(content_id, store) == ""
    store.mark_published(content_id)
    assert "đã đăng" in publish_block_reason(content_id, store).lower()


def test_replenish_writes_tomorrows_posts_before_the_posting_hour(tmp_path, monkeypatch) -> None:
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo

    store = _store(tmp_path)
    store.upsert_profile({"page_id": "page-br", "language": "pt-BR", "topic": ["một"], "status": "ACTIVE", "daily_content_target": 1})
    store.add_policy("page-br", {"language": "pt-BR", "requires_source": False, "requires_media": False}, requested_by="user")
    store.upsert_profile(
        {
            "page_id": "page-2",
            "language": "pt-BR",
            "topic": ["hai"],
            "status": "ACTIVE",
            "daily_content_target": 3,
            "posting_schedule": "08:00, 13:00, 19:00",
        }
    )
    store.add_policy("page-2", {"language": "pt-BR", "requires_source": False, "requires_media": False}, requested_by="user")
    store.set_emergency_stop(True)
    from src.services.ai_fleet.pipeline import replenish_fleet_posts

    assert replenish_fleet_posts(store, pages=[], posts=None, batch=1) == []
    store.set_emergency_stop(False)
    store.set_provider_choices(text="openai", image="openai", make_image=False)
    monkeypatch.setenv("FLEET_FILL_TODAY", "0")
    _Text.calls = 0
    monkeypatch.setattr("src.services.ai_fleet.pipeline.AIProviderFactory.text", lambda name: _Text())
    _allow_image(monkeypatch, tmp_path)
    tomorrow = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).date() + timedelta(days=1)
    already = datetime(tomorrow.year, tomorrow.month, tomorrow.day, 9, 0, tzinfo=ZoneInfo("Asia/Ho_Chi_Minh"))

    class Posts:
        def __init__(self) -> None:
            self.jobs = [
                {
                    "page_id": "page-br",
                    "fleet_content_id": "already",
                    "status": "pending",
                    "scheduled_at": already.isoformat(),
                }
            ]

        def load_all(self) -> list[dict]:
            return list(self.jobs)

        def upsert(self, row: dict) -> None:
            self.jobs.append(dict(row))

    posts = Posts()
    rows = replenish_fleet_posts(
        store,
        pages=[
            {"page_id": "page-br", "account_id": "acc-1", "page_name": "Brazil"},
            {"page_id": "page-2", "account_id": "acc-1", "page_name": "Hai"},
        ],
        posts=posts,
        batch=5,
    )
    scheduled = [row for row in rows if str(row["result"]).startswith("đã vào lịch")]
    assert [row["page_id"] for row in scheduled] == ["page-2", "page-2", "page-2"]
    created = [job for job in posts.jobs if job.get("page_id") == "page-2"]
    moments = [datetime.fromisoformat(job["scheduled_at"]).astimezone(ZoneInfo("Asia/Ho_Chi_Minh")) for job in created]
    assert [item.date() for item in moments] == [tomorrow, tomorrow, tomorrow]
    assert [(item.hour, item.minute) for item in moments] == [(8, 0), (13, 0), (19, 0)]
    assert all(item > datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")) for item in moments)


def test_replenish_rotates_accounts_and_does_not_rewrite_a_full_day(tmp_path, monkeypatch) -> None:
    store = _store(tmp_path)
    for page_id, topic in (("page-br", "một"), ("page-2", "hai")):
        store.upsert_profile(
            {
                "page_id": page_id,
                "language": "pt-BR",
                "topic": [topic],
                "status": "ACTIVE",
                "daily_content_target": 1,
                "posting_schedule": "09:00",
            }
        )
        store.add_policy(page_id, {"language": "pt-BR", "requires_source": False, "requires_media": False}, requested_by="user")
    store.set_provider_choices(text="openai", image="openai", make_image=False)
    store.set_selected_accounts(["key-a", "key-b"], [])
    monkeypatch.setenv("FLEET_FILL_TODAY", "0")
    used: list[int] = []
    monkeypatch.setattr("src.services.ai_fleet.pipeline.apply_account_rotation", lambda _store, index: used.append(index))
    monkeypatch.setattr("src.services.ai_fleet.pipeline.AIProviderFactory.text", lambda name: _Text())
    _allow_image(monkeypatch, tmp_path)
    _Text.calls = 0

    class Posts:
        def __init__(self) -> None:
            self.jobs: list[dict] = []

        def load_all(self) -> list[dict]:
            return list(self.jobs)

        def upsert(self, row: dict) -> None:
            self.jobs.append(dict(row))

    from src.services.ai_fleet.pipeline import replenish_fleet_posts

    posts = Posts()
    pages = [
        {"page_id": "page-br", "account_id": "acc-1", "page_name": "Brazil"},
        {"page_id": "missing", "account_id": "", "page_name": "Thiếu"},
        {"page_id": "page-2", "account_id": "acc-1", "page_name": "Hai"},
    ]
    store.upsert_profile({"page_id": "missing", "language": "pt-BR", "status": "ACTIVE", "daily_content_target": 1})
    first = replenish_fleet_posts(store, pages=pages, posts=posts, batch=5)
    assert [row["page_id"] for row in first if str(row["result"]).startswith("đã vào lịch")] == ["page-br", "page-2"]
    assert any(row["result"] == "Page chưa gắn tài khoản." for row in first)
    assert used == [0, 1]
    assert len(posts.jobs) == 2
    again = replenish_fleet_posts(store, pages=pages, posts=posts, batch=5)
    assert [row for row in again if str(row["result"]).startswith("đã vào lịch")] == []
    assert len(posts.jobs) == 2


def test_chatgpt_out_of_credit_writes_with_gemini(tmp_path, monkeypatch) -> None:
    store = _store(tmp_path)
    store.add_policy("page-br", {"language": "pt-BR", "requires_source": False, "requires_media": False}, requested_by="user")

    class OutOfCredit:
        provider_name = "openai"

        def generate_content_package(self, *, prompt: str, model: str | None = None) -> dict:
            raise RuntimeError("Tài khoản ChatGPT đã hết credit.")

    class GeminiWriter:
        provider_name = "gemini"

        def generate_content_package(self, *, prompt: str, model: str | None = None) -> dict:
            package = _ready("Bài viết bằng Gemini")
            package["provider"] = "gemini"
            return package

    monkeypatch.setattr(
        "src.services.ai_fleet.pipeline.AIProviderFactory.text",
        lambda name: OutOfCredit() if name == "openai" else GeminiWriter(),
    )
    with pytest.raises(RuntimeError, match="hết credit"):
        create_post(store, page_id="page-br", topic="phim", text_name="openai", image_name="openai", make_image=False)


def test_automatic_post_needs_an_image_before_approval(tmp_path, monkeypatch) -> None:
    store = _store(tmp_path)
    store.upsert_profile({"page_id": "page-br", "language": "pt-BR", "topic": ["một"], "status": "ACTIVE", "daily_content_target": 1})
    store.add_policy("page-br", {"language": "pt-BR", "requires_source": False, "requires_media": False}, requested_by="user")
    monkeypatch.setattr("src.services.ai_fleet.pipeline.AIProviderFactory.text", lambda name: _Text())
    monkeypatch.setattr("src.services.ai_fleet.pipeline._enable_saved_gemini", lambda: False)
    monkeypatch.setenv("FLEET_FILL_TODAY", "0")

    class Broken:
        def generate_images(self, *, prompt: str, number_of_images: int, model: str | None = None) -> list[bytes]:
            raise RuntimeError("Tạo ảnh lỗi")

    monkeypatch.setattr("src.services.ai_fleet.pipeline.AIProviderFactory.image", lambda name: Broken())

    class Posts:
        def __init__(self) -> None:
            self.jobs: list[dict] = []

        def load_all(self) -> list[dict]:
            return list(self.jobs)

        def upsert(self, row: dict) -> None:
            self.jobs.append(dict(row))

    from src.services.ai_fleet.pipeline import run_automatic

    posts = Posts()
    pages = [{"page_id": "page-br", "account_id": "acc-1", "page_name": "Brazil"}]
    blocked = run_automatic(store, pages=pages, text_name="openai", image_name="openai", make_image=True, posts=posts)
    assert "chưa có ảnh" in blocked[0]["result"]
    assert posts.jobs == []
    _allow_image(monkeypatch, tmp_path)
    approved = run_automatic(store, pages=pages, text_name="openai", image_name="openai", make_image=True, posts=posts)
    assert approved[0]["result"].startswith("đã vào lịch")
    saved = store.content(posts.jobs[0]["fleet_content_id"])
    assert saved["status"] == "APPROVED"
    assert saved["versions"][-1]["asset_ids"]


def test_rate_limit_writes_one_post_then_waits(tmp_path, monkeypatch) -> None:
    store = _store(tmp_path)
    store.upsert_profile({"page_id": "page-br", "language": "pt-BR", "topic": ["một", "hai"], "status": "ACTIVE", "daily_content_target": 3})
    store.add_policy("page-br", {"language": "pt-BR", "requires_source": False, "requires_media": False}, requested_by="user")
    store.mark_verified("openai", "key-ok")

    class Limited:
        provider_name = "openai"
        calls = 0

        def generate_content_package(self, *, prompt: str, model: str | None = None) -> dict:
            Limited.calls += 1
            raise RuntimeError("OpenAI text lỗi HTTP 429. Không đổi sang provider khác.")

    monkeypatch.setattr("src.services.ai_fleet.pipeline.AIProviderFactory.text", lambda name: Limited())
    monkeypatch.setenv("FLEET_FILL_TODAY", "0")

    class Posts:
        def load_all(self) -> list[dict]:
            return []

        def upsert(self, row: dict) -> None:
            raise AssertionError(row)

    from src.services.ai_fleet.pipeline import replenish_fleet_posts, run_automatic

    rows = run_automatic(
        store,
        pages=[{"page_id": "page-br", "account_id": "acc-1", "page_name": "Brazil"}],
        text_name="openai",
        image_name="openai",
        make_image=True,
        posts=Posts(),
    )
    assert Limited.calls == 1
    assert "429" in rows[0]["result"]
    store.note_attention("page-br", rows[0]["result"])
    Limited.calls = 0
    assert replenish_fleet_posts(
        store,
        pages=[{"page_id": "page-br", "account_id": "acc-1", "page_name": "Brazil"}],
        posts=Posts(),
        batch=1,
    ) == []
    assert Limited.calls == 0


def test_open_slots_cover_later_today_and_the_next_day(tmp_path, monkeypatch) -> None:
    from datetime import datetime
    from zoneinfo import ZoneInfo

    from src.services.ai_fleet.pipeline import _slots_needed

    monkeypatch.delenv("FLEET_FILL_TODAY", raising=False)
    store = _store(tmp_path)
    store.upsert_profile(
        {
            "page_id": "page-br",
            "language": "pt-BR",
            "status": "ACTIVE",
            "daily_content_target": 3,
            "posting_schedule": "08:00, 13:00, 19:00",
        }
    )
    now = datetime(2026, 10, 11, 10, 0, tzinfo=ZoneInfo("Asia/Ho_Chi_Minh"))

    class Posts:
        def load_all(self) -> list[dict]:
            return []

    slots = _slots_needed(store, Posts(), (store.load().get("profiles") or {})["page-br"], "page-br", now)
    assert [(item.day, item.hour) for item in slots] == [(11, 13), (11, 19), (12, 8), (12, 13), (12, 19)]


def test_a_failed_page_is_listed_until_the_next_quiet_cycle(tmp_path, monkeypatch) -> None:
    store = _store(tmp_path)
    store.upsert_profile({"page_id": "page-br", "language": "pt-BR", "topic": ["một"], "status": "ACTIVE", "daily_content_target": 1})
    store.add_policy("page-br", {"language": "pt-BR", "requires_source": False, "requires_media": False}, requested_by="user")
    store.set_provider_choices(text="openai", image="openai", make_image=False)
    monkeypatch.setenv("FLEET_FILL_TODAY", "0")

    class Broken:
        provider_name = "openai"

        def generate_content_package(self, *, prompt: str, model: str | None = None) -> dict:
            raise RuntimeError("sk-secret HTTP 503")

    monkeypatch.setattr("src.services.ai_fleet.pipeline.AIProviderFactory.text", lambda name: Broken())

    class Posts:
        def __init__(self) -> None:
            self.jobs: list[dict] = []

        def load_all(self) -> list[dict]:
            return list(self.jobs)

        def upsert(self, row: dict) -> None:
            self.jobs.append(dict(row))

    from src.services.ai_fleet.pipeline import replenish_fleet_posts

    posts = Posts()
    replenish_fleet_posts(store, pages=[{"page_id": "page-br", "account_id": "acc-1", "page_name": "Brazil"}], posts=posts, batch=1)
    notes = store.load().get("attention") or []
    assert notes and notes[0]["page_id"] == "page-br"
    assert "sk-secret" not in notes[0]["message"]
    monkeypatch.setattr("src.services.ai_fleet.pipeline.AIProviderFactory.text", lambda name: _Text())
    _allow_image(monkeypatch, tmp_path)
    _Text.calls = 0
    replenish_fleet_posts(store, pages=[{"page_id": "page-br", "account_id": "acc-1", "page_name": "Brazil"}], posts=posts, batch=1)
    assert store.load().get("attention") == []
    assert posts.jobs


def test_replenish_does_nothing_while_auto_is_off(tmp_path, monkeypatch) -> None:
    store = _store(tmp_path)
    store.upsert_profile({"page_id": "page-br", "language": "pt-BR", "topic": ["một"], "status": "ACTIVE", "daily_content_target": 1})
    store.add_policy("page-br", {"language": "pt-BR", "requires_source": False, "requires_media": False}, requested_by="user")
    store.set_auto_enabled(False)
    monkeypatch.setenv("FLEET_FILL_TODAY", "0")
    monkeypatch.setattr("src.services.ai_fleet.pipeline.AIProviderFactory.text", lambda name: _Text())

    class Posts:
        def load_all(self) -> list[dict]:
            return []

        def upsert(self, row: dict) -> None:
            raise AssertionError(row)

    from src.services.ai_fleet.pipeline import replenish_fleet_posts

    assert replenish_fleet_posts(store, pages=[{"page_id": "page-br", "account_id": "acc-1", "page_name": "Brazil"}], posts=Posts(), batch=1) == []


def test_edited_post_keeps_the_scheduled_hour_after_approval(tmp_path) -> None:
    from src.services.ai_fleet.gate import human_decide
    from src.services.ai_fleet.pipeline import save_post_edit, sync_approved_job

    store = _store(tmp_path)
    store.add_policy("page-br", {"language": "pt-BR", "requires_source": False, "requires_media": False}, requested_by="user")
    content = store.create_draft("page-br", _ready())
    content_id = content["content_id"]
    human_decide(store, content_id, "APPROVED")
    when = "2026-10-12T01:00:00+00:00"

    class Posts:
        def __init__(self) -> None:
            self.jobs = [
                {
                    "id": "job-1",
                    "page_id": "page-br",
                    "fleet_content_id": content_id,
                    "status": "pending",
                    "scheduled_at": when,
                    "title": "Phim hài cổ",
                    "content": "Xem lại phim hài cổ điển.",
                }
            ]

        def load_all(self) -> list[dict]:
            return list(self.jobs)

        def upsert(self, row: dict) -> None:
            self.jobs = [dict(row) if item.get("id") == row.get("id") else item for item in self.jobs]

    posts = Posts()
    edited = save_post_edit(store, content_id, title="Tiêu đề mới", body="Thân bài mới", caption="Caption đã sửa", hashtags=["#moi"])
    assert edited["changed"] is True
    assert schedule_block_reason(content_id, store)
    assert sync_approved_job(store, content_id, posts) == 0
    human_decide(store, content_id, "APPROVED")
    assert sync_approved_job(store, content_id, posts) == 1
    assert posts.jobs[0]["content"] == "Caption đã sửa"
    assert posts.jobs[0]["scheduled_at"] == when


def test_stable_post_stays_on_schedule_until_publish_now(tmp_path) -> None:
    from datetime import datetime, timezone

    from src.services.ai_fleet.gate import human_decide
    from src.services.ai_fleet.pipeline import place_approved_post

    store = _store(tmp_path)
    store.add_policy("page-br", {"language": "pt-BR", "requires_source": False, "requires_media": False}, requested_by="user")
    content = store.create_draft("page-br", _ready())
    content_id = content["content_id"]
    human_decide(store, content_id, "APPROVED")
    when = "2026-10-12T02:00:00+00:00"

    class Posts:
        def __init__(self) -> None:
            self.jobs = [
                {
                    "id": "job-1",
                    "page_id": "page-br",
                    "account_id": "acc-1",
                    "fleet_content_id": content_id,
                    "status": "pending",
                    "scheduled_at": when,
                    "title": "Phim hài cổ",
                    "content": "Xem lại phim hài cổ điển.",
                }
            ]
            self.writes = 0

        def load_all(self) -> list[dict]:
            return list(self.jobs)

        def upsert(self, row: dict) -> None:
            self.writes += 1
            self.jobs = [dict(row) if item.get("id") == row.get("id") else item for item in self.jobs]

    posts = Posts()
    assert place_approved_post(store, content_id, account_id="acc-1", posts=posts) == ""
    assert posts.writes == 0
    assert posts.jobs[0]["scheduled_at"] == when
    assert place_approved_post(store, content_id, account_id="acc-1", posts=posts, when=datetime.now(timezone.utc)) == ""
    assert posts.writes == 1
    assert posts.jobs[0]["id"] == "job-1"
    assert posts.jobs[0]["scheduled_at"] != when
    draft = store.create_draft("page-br", {**_ready("Bài chưa duyệt"), "caption": "Chưa duyệt bài này."})
    assert place_approved_post(store, draft["content_id"], account_id="acc-1", posts=posts)


def test_delete_one_post_while_others_are_not_approved(tmp_path) -> None:
    from src.utils.schedule_posts_manager import SchedulePostsManager

    store = _store(tmp_path)
    first = store.create_draft("page-br", _ready("Một"))
    second = store.create_draft("page-br", _ready("Hai"))
    when = "2026-10-13T02:00:00+00:00"
    posts = SchedulePostsManager(tmp_path / "schedule_posts.json")
    posts.save_all(
        [
            {"id": "job-1", "page_id": "page-br", "account_id": "acc", "fleet_content_id": first["content_id"], "status": "pending", "scheduled_at": when, "post_type": "text"},
            {"id": "job-2", "page_id": "page-br", "account_id": "acc", "fleet_content_id": second["content_id"], "status": "pending", "scheduled_at": "2026-10-13T03:00:00+00:00", "post_type": "text"},
        ],
        enforce_fleet_gate=False,
    )
    delete_post(store, first["content_id"], posts)
    left = posts.load_all()
    assert [job["id"] for job in left] == ["job-2"]
    assert store.content(first["content_id"]) is None
    assert store.content(second["content_id"]) is not None


def test_delete_post_removes_the_waiting_job(tmp_path) -> None:
    store = _store(tmp_path)
    content = store.create_draft("page-br", _ready())
    content_id = content["content_id"]

    class Posts:
        def __init__(self) -> None:
            self.jobs = [{"id": "job-1", "fleet_content_id": content_id, "status": "pending", "page_id": "page-br"}]

        def load_all(self) -> list[dict]:
            return list(self.jobs)

        def delete_by_ids(self, ids: list[str]) -> tuple[int, list[str]]:
            self.jobs = [job for job in self.jobs if job["id"] not in ids]
            return (1, [])

    posts = Posts()
    delete_post(store, content_id, posts)
    assert store.content(content_id) is None
    assert posts.jobs == []
    from datetime import datetime
    from zoneinfo import ZoneInfo

    from src.services.ai_fleet.pipeline import _slots_needed

    when = datetime(2026, 10, 13, 9, 0, tzinfo=ZoneInfo("Asia/Ho_Chi_Minh"))
    store.upsert_profile({"page_id": "page-br", "status": "ACTIVE", "daily_content_target": 1, "posting_schedule": "09:00"})
    kept = store.create_draft("page-br", _ready("Giữ giờ"))
    posts.jobs = [{"id": "job-2", "fleet_content_id": kept["content_id"], "status": "pending", "page_id": "page-br", "scheduled_at": when.isoformat()}]
    delete_post(store, kept["content_id"], posts)
    later = datetime(2026, 10, 12, 8, 0, tzinfo=ZoneInfo("Asia/Ho_Chi_Minh"))
    slots = _slots_needed(store, posts, store.load()["profiles"]["page-br"], "page-br", later)
    assert all(item.strftime("%Y-%m-%dT%H:%M") != "2026-10-13T09:00" for item in slots)
    published = store.create_draft("page-br", _ready("Đã đăng"))
    published["status"] = "PUBLISHED"
    published["published"] = True
    store.save_content(published)
    with pytest.raises(ValueError):
        delete_post(store, published["content_id"], posts)


def test_saved_hour_frame_runs_every_clock_each_day() -> None:
    from datetime import date, timezone

    from src.services.ai_fleet.pipeline import _posting_times

    times = _posting_times(
        {"posting_schedule": "21:00, 08:30", "daily_content_target": 1},
        date(2026, 10, 13),
        timezone.utc,
    )
    assert [(item.hour, item.minute) for item in times] == [(8, 30), (21, 0)]


def test_unknown_text_provider_is_refused() -> None:
    with pytest.raises(RuntimeError):
        text_provider("claude")


def test_review_note_stays_on_that_page_and_asset_locks_after_approval(tmp_path) -> None:
    store = _store(tmp_path)
    content = store.create_draft("page-br", _ready())
    from src.services.ai_fleet.gate import human_decide

    store.add_version(content["content_id"], {**_ready(), "asset_ids": ["poster.png"], "source_urls": ["https://example.com"]})
    review = human_decide(store, content["content_id"], "APPROVED")
    note_review(store, content["content_id"], review)
    report = page_report(store, "page-br")
    assert report["learned"] == 1
    with pytest.raises(ValueError):
        store.bind_asset(content["content_id"], "other.png", image_provider="openai")
