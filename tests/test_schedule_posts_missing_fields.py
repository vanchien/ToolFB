"""Unit-test cho phát hiện field thiếu + filter theo field thiếu + dependency order regen."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.utils.schedule_posts_missing_fields import (
    MISSING_FIELD_LABELS,
    REGENERABLE_FIELDS,
    filter_jobs_by_missing_fields,
    format_missing_fields_for_display,
    get_missing_fields,
    order_regenerable_fields,
    preset_by_label,
)


def _base(*, jid: str = "j1", **overrides) -> dict:
    base = {
        "id": jid,
        "account_id": "acc_1",
        "page_id": "page_A",
        "post_type": "text",
        "scheduled_at": "2026-04-22T00:00:00+00:00",
        "title": "Bài tốt",
        "content": "Nội dung đầy đủ",
        "hashtags": ["#ai"],
    }
    base.update(overrides)
    return base


# ---------- get_missing_fields ----------

def test_no_missing_fields_for_full_text_job() -> None:
    assert get_missing_fields(_base()) == []


def test_missing_title_only() -> None:
    j = _base(title="")
    assert get_missing_fields(j) == ["title"]


def test_missing_title_and_content() -> None:
    j = _base(title="   ", content="")
    miss = get_missing_fields(j)
    assert set(miss) == {"title", "content"}


def test_missing_hashtags_empty_list() -> None:
    j = _base(hashtags=[])
    assert "hashtags" in get_missing_fields(j)


def test_text_post_does_not_require_image_prompt() -> None:
    j = _base(post_type="text", image_prompt="")
    assert "image_prompt" not in get_missing_fields(j)
    assert "image_path" not in get_missing_fields(j)


def test_text_image_post_requires_image_prompt_and_path() -> None:
    j = _base(post_type="text_image", image_prompt="", media_files=[])
    miss = get_missing_fields(j)
    assert "image_prompt" in miss
    assert "image_path" in miss


def test_image_path_flagged_when_file_missing(tmp_path: Path) -> None:
    # media_files trỏ tới path không tồn tại → vẫn bị coi là thiếu
    fake = str(tmp_path / "no_such_file.png")
    j = _base(post_type="text_image", image_prompt="ok", media_files=[fake])
    assert "image_path" in get_missing_fields(j)


def test_image_path_ok_when_file_exists(tmp_path: Path) -> None:
    f = tmp_path / "real.png"
    f.write_bytes(b"x")
    j = _base(post_type="text_image", image_prompt="ok", media_files=[str(f)])
    assert "image_path" not in get_missing_fields(j)


def test_video_job_requires_video_path(tmp_path: Path) -> None:
    bad = _base(post_type="video", media_files=[], content="")
    miss = get_missing_fields(bad)
    assert "video_path" in miss
    v = tmp_path / "v.mp4"
    v.write_bytes(b"x")
    good = _base(post_type="video", media_files=[str(v)], content="")
    assert "video_path" not in get_missing_fields(good)


def test_critical_fields_flagged() -> None:
    j = _base()
    j.pop("page_id")
    j["account_id"] = ""
    j["scheduled_at"] = ""
    miss = get_missing_fields(j)
    assert "page_id" in miss
    assert "account_id" in miss
    assert "scheduled_at" in miss


# ---------- filter_jobs_by_missing_fields ----------

def test_filter_none_returns_all() -> None:
    jobs = [_base(title=""), _base(title="ok")]
    out = filter_jobs_by_missing_fields(jobs, [], match_mode="none")
    assert len(out) == 2


def test_filter_any_mode() -> None:
    jobs = [
        _base(jid="a", title="", content="c"),
        _base(jid="b", title="t", content=""),
        _base(jid="c", title="t", content="c"),
    ]
    out = filter_jobs_by_missing_fields(jobs, ["title", "content"], match_mode="any")
    ids = {j["id"] for j in out}
    assert ids == {"a", "b"}


def test_filter_all_mode_requires_every_field_missing() -> None:
    jobs = [
        _base(jid="a", title="", content=""),
        _base(jid="b", title="", content="c"),
        _base(jid="c", title="t", content=""),
    ]
    out = filter_jobs_by_missing_fields(jobs, ["title", "content"], match_mode="all")
    ids = {j["id"] for j in out}
    assert ids == {"a"}


def test_presets_cover_labels() -> None:
    assert "Không lọc" in MISSING_FIELD_LABELS
    p = preset_by_label("Thiếu title + content")
    assert p["match_mode"] == "all"
    assert set(p["fields"]) == {"title", "content"}


# ---------- order_regenerable_fields ----------

def test_order_regenerable_fields_is_dependency_safe() -> None:
    got = order_regenerable_fields(["image_path", "title", "image_prompt", "hashtags", "content"])
    assert got == ["title", "content", "hashtags", "image_prompt", "image_path"]


def test_order_strips_non_regenerable() -> None:
    got = order_regenerable_fields(["title", "page_id", "scheduled_at"])
    assert got == ["title"]


def test_regenerable_fields_contract() -> None:
    assert REGENERABLE_FIELDS == (
        "title",
        "content",
        "hashtags",
        "image_prompt",
        "image_path",
    )


# ---------- format_missing_fields_for_display ----------

def test_format_empty_returns_blank() -> None:
    assert format_missing_fields_for_display([]) == ""


def test_format_truncates_long_list() -> None:
    out = format_missing_fields_for_display(
        ["title", "content", "hashtags", "image_prompt", "image_path"],
        max_chars=20,
    )
    assert len(out) <= 20
    assert out.endswith("…")


# ---------- regenerate_missing_fields_for_job dependency order (no Gemini) ----------

def test_regenerate_does_nothing_when_no_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    from src.services import job_field_regenerator as mod

    # Kể cả nếu hàm con bị gọi → test sẽ fail. Không stub gì cả.
    updated, regen = mod.regenerate_missing_fields_for_job(_base())
    assert regen == []
    assert updated["title"] == "Bài tốt"


def test_regenerate_only_missing_fields_preserves_others(monkeypatch: pytest.MonkeyPatch) -> None:
    """Khi chỉ thiếu ``hashtags`` → chỉ ``_regen_hashtags`` được gọi,
    các helper khác KHÔNG được gọi; các field sẵn được giữ nguyên."""
    from src.services import job_field_regenerator as mod

    called: dict[str, int] = {"title": 0, "content": 0, "hashtags": 0, "image_prompt": 0, "image_path": 0}

    def fake_title(_j):  # pragma: no cover — must NOT be called
        called["title"] += 1
        return "SHOULD_NOT_BE_USED"

    def fake_content(_j):  # pragma: no cover
        called["content"] += 1
        return ("nope", "")

    def fake_hashtags(_j):
        called["hashtags"] += 1
        return ["#ai", "#regen"]

    def fake_prompt(_j):  # pragma: no cover
        called["image_prompt"] += 1
        return "nope"

    def fake_image(_j):  # pragma: no cover
        called["image_path"] += 1
        return []

    monkeypatch.setattr(mod, "_regen_title", fake_title)
    monkeypatch.setattr(mod, "_regen_content", fake_content)
    monkeypatch.setattr(mod, "_regen_hashtags", fake_hashtags)
    monkeypatch.setattr(mod, "_regen_image_prompt", fake_prompt)
    monkeypatch.setattr(mod, "_regen_image_path", fake_image)

    job = _base(hashtags=[])
    updated, regen = mod.regenerate_missing_fields_for_job(job)
    assert regen == ["hashtags"]
    assert called == {"title": 0, "content": 0, "hashtags": 1, "image_prompt": 0, "image_path": 0}
    assert updated["title"] == "Bài tốt"  # preserved
    assert updated["content"] == "Nội dung đầy đủ"  # preserved
    assert updated["hashtags"] == ["#ai", "#regen"]


def test_regenerate_respects_allowed_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    """``allowed_fields=['title']`` → dù ``content`` cũng thiếu cũng không sinh."""
    from src.services import job_field_regenerator as mod

    monkeypatch.setattr(mod, "_regen_title", lambda _j: "Tiêu đề mới")
    monkeypatch.setattr(mod, "_regen_content", lambda _j: pytest.fail("không được gọi"))
    monkeypatch.setattr(mod, "_regen_hashtags", lambda _j: pytest.fail("không được gọi"))

    job = _base(title="", content="")
    updated, regen = mod.regenerate_missing_fields_for_job(job, allowed_fields=["title"])
    assert regen == ["title"]
    assert updated["title"] == "Tiêu đề mới"
    assert updated["content"] == ""  # vẫn giữ (không patch)


def test_regenerate_dependency_order_title_then_content_then_hashtags(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.services import job_field_regenerator as mod

    order: list[str] = []

    def fake_title(_j):
        order.append("title")
        return "T"

    def fake_content(j):
        order.append("content")
        assert j["title"] == "T", "title phải được patch vào job TRƯỚC khi sinh content"
        return ("BODY", "alt")

    def fake_hashtags(j):
        order.append("hashtags")
        assert j["content"] == "BODY", "content phải có trước khi sinh hashtags"
        return ["#x"]

    monkeypatch.setattr(mod, "_regen_title", fake_title)
    monkeypatch.setattr(mod, "_regen_content", fake_content)
    monkeypatch.setattr(mod, "_regen_hashtags", fake_hashtags)

    job = _base(title="", content="", hashtags=[])
    updated, regen = mod.regenerate_missing_fields_for_job(job)
    assert regen == ["title", "content", "hashtags"]
    assert order == ["title", "content", "hashtags"]
    assert updated["title"] == "T"
    assert updated["content"] == "BODY"
    assert updated["hashtags"] == ["#x"]


def test_regenerate_protects_critical_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    from src.services import job_field_regenerator as mod

    job = _base()
    job["scheduled_at"] = ""  # critical missing
    # Không stub — nếu có field nào cần regen sẽ gọi Gemini. Không có field regenerable nào thiếu.
    updated, regen = mod.regenerate_missing_fields_for_job(job)
    assert regen == []
    assert updated["scheduled_at"] == ""  # không bị đụng
