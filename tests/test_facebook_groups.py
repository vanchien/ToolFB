"""Acceptance test Group Discovery, Join và Link Share. Không gọi Facebook."""

from __future__ import annotations

import random
import threading
import time

from src.services.facebook_groups.classify import (
    classify_membership_text,
    classify_posting_text,
    join_action_from_label,
    parse_group_search_html,
    inspect_member_count,
    parse_member_count,
    parse_min_members,
)
from src.services.facebook_groups.facebook_provider import FacebookGroupProvider
from src.services.facebook_groups.engine import GroupEngine
from src.services.facebook_groups.fingerprint import assert_link_only, classify_source_url, normalize_share_images
from src.services.facebook_groups.match import membership_bucket, qualify_group
from src.services.facebook_groups.store import GroupStore


class FakeGroups:
    def __init__(self) -> None:
        self.session = ""
        self.groups = []
        self.membership = {}
        self.join_calls = []
        self.publish_calls = []
        self.verify_posts = {}
        self.sleeps = []
        self.search_calls = []

    def check_session(self, account_id: str) -> str:
        del account_id
        return self.session

    def search_groups(self, **kwargs):
        self.search_calls.append(dict(kwargs))
        keyword = kwargs.get("keyword") or ""
        rows = []
        for item in self.groups:
            row = dict(item)
            words = list(row.get("matched_keywords") or [])
            if keyword and keyword not in words:
                words.append(keyword)
            row["matched_keywords"] = words
            row["country"] = kwargs.get("country") or row.get("country") or ""
            row["language"] = kwargs.get("language") or row.get("language") or ""
            rows.append(row)
        return {"error_code": "", "groups": rows}

    def check_membership(self, *, account_id: str, page_id: str, group_id: str):
        del account_id, page_id
        return dict(self.membership.get(group_id) or {"membership_status": "UNKNOWN", "approval_status": "UNKNOWN"})

    def request_join_if_available(self, *, account_id: str, page_id: str, group_id: str):
        self.join_calls.append(group_id)
        return self.check_membership(account_id=account_id, page_id=page_id, group_id=group_id)

    def verify_membership(self, *, account_id: str, page_id: str, group_id: str):
        row = self.check_membership(account_id=account_id, page_id=page_id, group_id=group_id)
        if group_id in getattr(self, "verify_joined", set()):
            row["membership_status"] = "JOINED"
            row["approval_status"] = "NO_APPROVAL_INDICATED"
        return row

    def publish_link(self, **kwargs):
        self.publish_calls.append(dict(kwargs))
        assert "video_path" not in kwargs
        group_id = kwargs["group_id"]
        if group_id in getattr(self, "timeout_once", set()):
            self.timeout_once.remove(group_id)
            return {"error_code": "TIMEOUT", "post_id": ""}
        return {"post_id": f"post-{group_id}", "post_url": f"https://www.facebook.com/{group_id}/posts/1"}

    def verify_published_post(self, **kwargs):
        group_id = kwargs["group_id"]
        post_id = self.verify_posts.get(group_id, "")
        return {"post_id": post_id, "post_url": f"https://www.facebook.com/{post_id}" if post_id else ""}


def _engine(tmp_path, provider=None):
    store = GroupStore(tmp_path / "facebook_groups.json")
    fake = provider or FakeGroups()
    engine = GroupEngine(store, fake, sleep=lambda seconds: fake.sleeps.append(seconds))
    return store, engine, fake


def test_01_search_saves_groups(tmp_path) -> None:
    store, engine, fake = _engine(tmp_path)
    fake.groups = [
        {"group_id": "111", "group_name": "Brazil Comedy", "group_url": "https://www.facebook.com/groups/111"}
    ]
    job = store.add_discovery_job(
        {"topic": "Movies", "keywords": ["comedy"], "countries": ["BR"], "languages": ["pt"], "account_id": "acc"}
    )
    done = engine.run_discovery(job["id"])
    assert done["status"] == "COMPLETED"
    assert store.group_by_facebook_id("111")["group_name"] == "Brazil Comedy"


def test_02_ten_keywords_one_group_id(tmp_path) -> None:
    store, engine, fake = _engine(tmp_path)
    fake.groups = [{"group_id": "123", "group_name": "Brazil Comedy kw0", "group_url": "https://www.facebook.com/groups/123"}]
    job = store.add_discovery_job(
        {
            "topic": "Movies",
            "keywords": [f"kw{i}" for i in range(10)],
            "countries": ["BR"],
            "languages": ["pt"],
        }
    )
    engine.run_discovery(job["id"])
    assert len(store.groups()) == 1
    words = store.group_by_facebook_id("123")["matched_keywords"]
    assert all(f"kw{i}" in words for i in range(10))


def test_03_filter_joined_and_can_post(tmp_path) -> None:
    store, engine, _fake = _engine(tmp_path)
    store.upsert_group({"group_id": "a", "group_name": "Joined", "country": "BR", "language": "pt"})
    store.upsert_group({"group_id": "b", "group_name": "Other", "country": "BR", "language": "pt"})
    store.upsert_membership(
        {"account_id": "acc", "page_id": "", "group_id": "a", "membership_status": "JOINED", "posting_permission": "ALLOWED", "approval_status": "NO_APPROVAL_INDICATED"}
    )
    store.upsert_membership(
        {"account_id": "acc", "page_id": "", "group_id": "b", "membership_status": "NOT_JOINED", "posting_permission": "UNKNOWN", "approval_status": "NO_APPROVAL_INDICATED"}
    )
    rows = engine.visible_groups({"membership": {"joined": True}, "posting": {"can_post": True}}, account_id="acc", page_id="")
    assert [row["group_id"] for row in rows] == ["a"]


def test_04_approval_required_is_not_joinable() -> None:
    assert membership_bucket("NOT_JOINED", "APPROVAL_REQUIRED") == "approval"
    membership, approval = classify_membership_text("Request to join this group")
    assert approval == "APPROVAL_REQUIRED"
    assert membership_bucket(membership, approval) != "joinable"


def test_05_unknown_is_not_no_approval() -> None:
    assert membership_bucket("UNKNOWN", "UNKNOWN") == "unknown"
    assert membership_bucket("NOT_JOINED", "UNKNOWN") != "joinable"
    assert classify_posting_text("group feed", membership_status="JOINED") == "UNKNOWN"


def test_06_join_queue_is_sequential(tmp_path) -> None:
    store, engine, fake = _engine(tmp_path)
    fake.verify_joined = {f"g{i}" for i in range(10)}
    for index in range(10):
        fake.membership[f"g{index}"] = {"membership_status": "NOT_JOINED", "approval_status": "NO_APPROVAL_INDICATED"}
        engine.enqueue_joins(account_id="acc", page_id="", group_ids=[f"g{index}"])
    engine.run_join_queue("acc")
    assert fake.join_calls == [f"g{i}" for i in range(10)]
    assert len(fake.sleeps) == 9


def test_07_one_join_failure_continues(tmp_path) -> None:
    store, engine, fake = _engine(tmp_path)
    fake.membership["bad"] = {"membership_status": "NOT_JOINED", "approval_status": "NO_APPROVAL_INDICATED", "error_code": "PERMISSION_DENIED"}
    fake.membership["good"] = {"membership_status": "JOINED", "approval_status": "NO_APPROVAL_INDICATED", "posting_permission": "ALLOWED"}
    engine.enqueue_joins(account_id="acc", page_id="", group_ids=["bad", "good"])
    done = engine.run_join_queue("acc", continue_on_error=True)
    assert done[0]["status"] == "FAILED"
    assert done[1]["status"] == "ALREADY_JOINED"


def test_08_session_expired_waits_reauth(tmp_path) -> None:
    store, engine, fake = _engine(tmp_path)
    fake.session = "SESSION_EXPIRED"
    engine.enqueue_joins(account_id="acc1", page_id="", group_ids=["g1"])
    done = engine.run_join_queue("acc1")
    assert done[0]["status"] == "WAITING_REAUTH"
    assert fake.join_calls == []


def test_09_captcha_pauses(tmp_path) -> None:
    _store, engine, fake = _engine(tmp_path)
    fake.membership["g1"] = {"error_code": "CAPTCHA", "membership_status": "UNKNOWN", "approval_status": "UNKNOWN"}
    engine.enqueue_joins(account_id="acc", page_id="", group_ids=["g1"])
    done = engine.run_join_queue("acc")
    assert done[0]["status"] == "PAUSED"
    assert done[0]["error_code"] == "CAPTCHA"


def test_10_rate_limit_pauses(tmp_path) -> None:
    _store, engine, fake = _engine(tmp_path)
    fake.session = "RATE_LIMITED"
    engine.enqueue_joins(account_id="acc", page_id="", group_ids=["g1"])
    done = engine.run_join_queue("acc")
    assert done[0]["status"] == "PAUSED"
    assert done[0]["error_code"] == "RATE_LIMITED"


def test_11_ten_groups_create_ten_share_jobs(tmp_path) -> None:
    store, engine, _fake = _engine(tmp_path)
    ids = [f"g{i}" for i in range(10)]
    for group_id in ids:
        store.upsert_group({"group_id": group_id, "group_name": group_id})
        store.upsert_membership(
            {"account_id": "acc", "page_id": "page1", "group_id": group_id, "membership_status": "JOINED", "posting_permission": "ALLOWED", "approval_status": "NO_APPROVAL_INDICATED"}
        )
    result = engine.create_share_batch(
        account_id="acc",
        page_id="page1",
        source_url="https://www.facebook.com/videos/123",
        text="Video mới",
        group_ids=ids,
    )
    assert len(result["created"]) == 10
    assert classify_source_url("https://www.facebook.com/videos/123") == "FACEBOOK_VIDEO_URL"
    assert all("video_path" not in job for job in store.share_jobs())


def test_12_publisher_sends_url_and_text_only(tmp_path) -> None:
    store, engine, fake = _engine(tmp_path)
    store.upsert_membership(
        {"account_id": "acc", "page_id": "", "group_id": "g1", "membership_status": "JOINED", "posting_permission": "ALLOWED"}
    )
    result = engine.create_share_batch(
        account_id="acc",
        page_id="",
        source_url="https://www.facebook.com/posts/1",
        text="Xem tại đây",
        group_ids=["g1"],
        cooldown_fixed=0,
    )
    engine.run_share_batch(result["batch"]["id"])
    assert fake.publish_calls[0]["source_url"].startswith("https://")
    assert fake.publish_calls[0]["text"] == "Xem tại đây"
    assert "video_path" not in fake.publish_calls[0]
    try:
        assert_link_only({"source_url": "https://x", "video_path": "a.mp4"})
        raised = False
    except ValueError:
        raised = True
    assert raised


def test_13_duplicate_share_is_not_created_again(tmp_path) -> None:
    store, engine, fake = _engine(tmp_path)
    store.upsert_membership(
        {"account_id": "acc", "page_id": "page1", "group_id": "g1", "membership_status": "JOINED", "posting_permission": "ALLOWED"}
    )
    first = engine.create_share_batch(
        account_id="acc", page_id="page1", source_url="https://www.facebook.com/videos/9", text="cùng chữ", group_ids=["g1"], cooldown_fixed=0
    )
    engine.run_share_batch(first["batch"]["id"])
    second = engine.create_share_batch(
        account_id="acc", page_id="page1", source_url="https://www.facebook.com/videos/9", text="cùng chữ", group_ids=["g1"], cooldown_fixed=0
    )
    assert second["duplicates"] == ["g1"]
    assert second["created"] == []
    assert sum(1 for job in store.share_jobs() if job["status"] == "DUPLICATE") == 1


def test_14_restart_verifies_before_repost(tmp_path) -> None:
    store, engine, fake = _engine(tmp_path)
    store.upsert_membership(
        {"account_id": "acc", "page_id": "", "group_id": "g1", "membership_status": "JOINED", "posting_permission": "ALLOWED"}
    )
    created = engine.create_share_batch(
        account_id="acc", page_id="", source_url="https://www.facebook.com/watch?v=1", text="a", group_ids=["g1"], cooldown_fixed=0
    )
    job = store.share_jobs()[0]
    job["status"] = "SUBMIT"
    store.save_share_job(job)
    fake.verify_posts["g1"] = "found-1"
    reloaded = GroupEngine(GroupStore(store.path), fake, sleep=lambda _s: None)
    assert reloaded.recover()["share"] == 1
    reloaded.run_share_batch(created["batch"]["id"])
    assert fake.publish_calls == []
    assert store.share_jobs()[0]["status"] == "COMPLETED"
    assert store.share_jobs()[0]["post_id"] == "found-1"


def test_recover_orphan_running_batch_becomes_ready(tmp_path) -> None:
    """Tắt app giữa chừng: batch RUNNING mồ côi → READY, không khóa tài khoản."""
    store, engine, fake = _engine(tmp_path)
    wave = engine.create_share_wave(
        pages=[
            {"account_id": "acc-a", "page_id": "p1", "page_name": "P1", "target_url": "https://www.facebook.com/p1"},
            {"account_id": "acc-a", "page_id": "p2", "page_name": "P2", "target_url": "https://www.facebook.com/p2"},
        ],
        source_url="https://www.facebook.com/reel/999",
        text="x",
        destination="page",
        cooldown_fixed=0,
    )
    batch_id = wave["batch_ids"][0]
    batch = next(b for b in store.share_batches() if str(b.get("id")) == batch_id)
    batch["status"] = "RUNNING"
    store.save_share_batch(batch)
    jobs = [j for j in store.share_jobs() if str(j.get("batch_id")) == batch_id]
    jobs[0]["status"] = "COMPLETED"
    store.save_share_job(jobs[0])
    reloaded = GroupEngine(GroupStore(store.path), fake, sleep=lambda _s: None)
    counts = reloaded.recover()
    assert counts["share_batches"] == 1
    restored = next(b for b in reloaded.store.share_batches() if str(b.get("id")) == batch_id)
    assert restored["status"] == "READY"
    assert reloaded.incomplete_share_batch_ids_for_url("https://www.facebook.com/reel/999") == [batch_id]
    # Batch mồ côi không làm _publish_busy khóa tài khoản.
    assert reloaded._publish_busy("acc-a", "", except_batch="other") is False
    reloaded.run_share_wave([batch_id])
    assert any(c["page_id"] == "p2" for c in fake.publish_calls)


def test_15_queue_persists_progress_cooldown_and_recovery(tmp_path) -> None:
    store, engine, fake = _engine(tmp_path)
    ids = [f"g{i}" for i in range(50)]
    for group_id in ids:
        store.upsert_membership(
            {"account_id": "acc", "page_id": "page1", "group_id": group_id, "membership_status": "JOINED", "posting_permission": "ALLOWED"}
        )
    created = engine.create_share_batch(
        account_id="acc",
        page_id="page1",
        source_url="https://example.com/watch",
        text="hello",
        group_ids=ids,
        cooldown_mode="fixed",
        cooldown_fixed=5,
    )
    engine.run_share_batch(created["batch"]["id"])
    again = GroupStore(store.path)
    assert len(again.share_jobs()) == 50
    batch = again.share_batches()[0]
    assert batch["completed_jobs"] == 50
    assert batch["status"] == "COMPLETED"
    assert fake.sleeps == [5] * 49
    board = engine.dashboard()
    assert board["unique_groups"] == 0 or board["share_batches"] == 1


def test_page_permission_is_not_copied_from_account(tmp_path) -> None:
    store, engine, _fake = _engine(tmp_path)
    store.upsert_membership(
        {"account_id": "acc", "page_id": "", "group_id": "g1", "membership_status": "JOINED", "posting_permission": "ALLOWED"}
    )
    result = engine.create_share_batch(
        account_id="acc", page_id="page1", source_url="https://www.facebook.com/posts/1", text="hi", group_ids=["g1"]
    )
    assert result["created"] == []
    assert result["skipped"] == ["g1"]


def test_other_account_continues_when_one_is_paused(tmp_path) -> None:
    store, engine, fake = _engine(tmp_path)
    fake.session = "CHECKPOINT"
    engine.enqueue_joins(account_id="acc1", page_id="", group_ids=["g1"])
    paused = engine.run_join_queue("acc1")
    assert paused[0]["status"] == "PAUSED"
    fake.session = ""
    fake.membership["g2"] = {"membership_status": "JOINED", "approval_status": "NO_APPROVAL_INDICATED"}
    engine.enqueue_joins(account_id="acc2", page_id="", group_ids=["g2"])
    done = engine.run_join_queue("acc2")
    assert done[0]["status"] == "ALREADY_JOINED"


def test_search_once_per_keyword_not_per_locale(tmp_path) -> None:
    store, engine, fake = _engine(tmp_path)
    fake.groups = [{"group_id": "111", "group_name": "Comedy", "group_url": "https://www.facebook.com/groups/111"}]
    job = store.add_discovery_job(
        {
            "topic": "Movies",
            "keywords": ["comedy"],
            "countries": ["BR", "PT"],
            "languages": ["pt", "en"],
            "account_id": "acc",
        }
    )
    engine.run_discovery(job["id"])
    assert len(fake.search_calls) == 2
    assert {item["keyword"] for item in fake.search_calls} == {"Movies", "comedy"}
    assert {item.get("place") or "" for item in fake.search_calls} == {""}


def test_join_request_label_is_not_a_click() -> None:
    assert join_action_from_label("Join") == "join"
    assert join_action_from_label("Tham gia nhóm") == "join"
    assert join_action_from_label("Request to join") == "approval"
    assert join_action_from_label("Yêu cầu tham gia") == "approval"


def test_pending_join_is_not_queued_twice(tmp_path) -> None:
    store, engine, _fake = _engine(tmp_path)
    first = engine.enqueue_joins(account_id="acc", page_id="", group_ids=["g1", "g1"])
    second = engine.enqueue_joins(account_id="acc", page_id="", group_ids=["g1"])
    assert len(first) == 1
    assert second == []


def test_parse_keeps_many_groups_and_skips_menu_links() -> None:
    links = "".join(f'<a href="/groups/{100000 + index}" aria-label="Film {index}">x</a>' for index in range(200))
    escaped = '<script>{"url":"https:\\/\\/www.facebook.com\\/groups\\/99887766\\/"}</script>'
    noise = '<a href="/groups/discover">Discover</a><a href="/groups/feed">Feed</a>'
    rows = parse_group_search_html(links + escaped + noise, keyword="film", country="BR")
    ids = {row["group_id"] for row in rows}
    assert len(rows) == 201
    assert "99887766" in ids
    assert "discover" not in ids
    assert "feed" not in ids
    assert rows[0]["group_name"] == "Film 0"


class _GrowingSearchPage:
    def __init__(self) -> None:
        self.step = 0
        self.url = "https://www.facebook.com/search/groups/?q=film"

    def content(self) -> str:
        count = min(30, (self.step + 1) * 10)
        return "".join(
            f'<a href="/groups/{5000 + index}" aria-label="Clip {index}">Clip {index}</a>' for index in range(count)
        )

    def evaluate(self, script: str):
        if "scrollBy" in script and self.step < 3:
            self.step += 1
        return []

    def wait_for_timeout(self, ms: int) -> None:
        del ms


def test_scroll_collects_groups_until_the_page_stops_growing() -> None:
    provider = FacebookGroupProvider()
    code, groups = provider._scroll_group_results(_GrowingSearchPage(), keyword="film", country="BR", language="en")
    assert code == ""
    assert len(groups) == 30


def test_script_captcha_word_does_not_stop_the_scan() -> None:
    class Page(_GrowingSearchPage):
        def content(self) -> str:
            return super().content() + "<script>recaptcha captcha</script>"

        def inner_text(self, selector: str) -> str:
            del selector
            return "Public groups about film"

    provider = FacebookGroupProvider()
    code, groups = provider._scroll_group_results(Page(), keyword="film", country="BR", language="en")
    assert code == ""
    assert len(groups) == 30


def test_visible_captcha_still_pauses() -> None:
    class Page:
        url = "https://www.facebook.com/checkpoint/"

        def content(self) -> str:
            return "<html>captcha</html>"

        def inner_text(self, selector: str) -> str:
            del selector
            return "I'm not a robot"

        def evaluate(self, script: str):
            del script
            return []

        def wait_for_timeout(self, ms: int) -> None:
            del ms

    provider = FacebookGroupProvider()
    code, groups = provider._scroll_group_results(Page(), keyword="film", country="", language="")
    assert code == "CAPTCHA"
    assert groups == []


def test_member_minimum_keeps_only_large_groups(tmp_path) -> None:
    store, engine, fake = _engine(tmp_path)
    fake.groups = [
        {"group_id": "small", "group_name": "Small film", "group_url": "https://www.facebook.com/groups/small", "member_count": 800},
        {"group_id": "large", "group_name": "Large film", "group_url": "https://www.facebook.com/groups/large", "member_count": 25000},
    ]
    job = store.add_discovery_job({"topic": "film", "keywords": ["film"], "account_id": "acc", "min_members": 10000})
    done = engine.run_discovery(job["id"])
    assert done["status"] == "COMPLETED"
    assert store.group_by_facebook_id("large")["member_count"] == 25000
    assert store.group_by_facebook_id("small") is None
    assert "giữ 1 nhóm" in done["note"]


def test_member_count_text_and_minimum_suffix() -> None:
    assert parse_member_count("Public · 12K members") == 12_000
    assert parse_member_count("1.2M members") == 1_200_000
    assert parse_member_count("10,540 thành viên") == 10540
    assert parse_min_members("10000") == 10_000
    assert parse_min_members("10k") == 10_000
    assert parse_min_members("10000k") == 10_000_000
    assert parse_member_count("25.5K members") == 25_500
    assert parse_member_count("10M members") == 10_000_000
    assert inspect_member_count("about 12K members") == (12_000, "APPROXIMATE")
    assert inspect_member_count("no count here") == (None, "UNKNOWN")


def _film_group(group_id: str, count, status: str = "EXACT") -> dict:
    return {
        "group_id": group_id,
        "group_name": "Filmes Brasil",
        "description": "cinema",
        "country": "Brazil",
        "language": "Portuguese",
        "member_count": count,
        "member_count_status": status,
    }


def test_minimum_members_has_no_upper_limit() -> None:
    base = dict(topic="film", keywords=["filmes"], countries=["Brazil"], languages=["Portuguese"])
    assert qualify_group(_film_group("a", 5000), min_members=10000, strict_member_count=True, **base)[1] == "MEMBER_COUNT_TOO_LOW"
    assert qualify_group(_film_group("b", 10000), min_members=10000, strict_member_count=True, **base)[0] is True
    assert qualify_group(_film_group("c", 50000), min_members=10000, strict_member_count=True, **base)[0] is True
    assert qualify_group(_film_group("d", 1_000_000), min_members=10000, strict_member_count=True, **base)[0] is True
    assert qualify_group(_film_group("e", 10_000_000), min_members=10000, strict_member_count=True, **base)[0] is True
    assert qualify_group(_film_group("f", None, "UNKNOWN"), min_members=10000, strict_member_count=True, **base)[1] == "MEMBER_COUNT_UNKNOWN"
    assert qualify_group(_film_group("g", 5000), min_members=None, strict_member_count=True, **base)[0] is True
    assert qualify_group(_film_group("h", 20000, "APPROXIMATE"), min_members=10000, strict_member_count=True, **base)[1] == "MEMBER_COUNT_APPROXIMATE"
    kept, reason = qualify_group(
        _film_group("i", 20000, "APPROXIMATE"),
        min_members=10000,
        strict_member_count=False,
        **base,
    )
    assert kept is True and reason == ""
    cars = _film_group("j", 80000)
    cars["group_name"] = "Brazil Cars Community"
    cars["description"] = ""
    assert qualify_group(cars, min_members=10000, strict_member_count=True, **base)[1] == "TOPIC_NOT_MATCHED"


def test_share_page_post_with_user_images_and_joined_groups(tmp_path) -> None:
    store, engine, fake = _engine(tmp_path)
    image = tmp_path / "cover.jpg"
    image.write_bytes(b"jpg")
    assert normalize_share_images([str(image)]) == [str(image)]
    try:
        normalize_share_images([str(tmp_path / "clip.mp4")])
        rejected = False
    except ValueError:
        rejected = True
    assert rejected
    page_job = engine.create_share_batch(
        account_id="acc",
        page_id="page1",
        source_url="https://www.facebook.com/other/posts/1",
        text="Xem bài này",
        image_paths=[str(image)],
        destination="page",
        target_url="https://www.facebook.com/mypage",
        cooldown_fixed=0,
    )
    assert len(page_job["created"]) == 1
    assert page_job["created"][0]["destination"] == "page"
    assert page_job["created"][0]["image_paths"] == [str(image)]
    engine.run_share_batch(page_job["batch"]["id"])
    assert fake.publish_calls[0]["image_paths"] == [str(image)]
    assert "video_path" not in fake.publish_calls[0]
    store.upsert_membership(
        {
            "account_id": "acc",
            "page_id": "page1",
            "group_id": "g-joined",
            "membership_status": "JOINED",
            "posting_permission": "UNKNOWN",
        }
    )
    store.upsert_membership(
        {
            "account_id": "acc",
            "page_id": "page1",
            "group_id": "g-out",
            "membership_status": "NOT_JOINED",
            "posting_permission": "UNKNOWN",
        }
    )
    groups = engine.create_share_batch(
        account_id="acc",
        page_id="page1",
        source_url="https://www.facebook.com/other/posts/1",
        text="Xem bài này",
        image_paths=[str(image)],
        group_ids=["g-joined", "g-out"],
        destination="joined",
        cooldown_fixed=0,
    )
    assert [job["group_id"] for job in groups["created"]] == ["g-joined"]
    assert "g-out" in groups["skipped"]


def test_share_wave_posts_into_typed_group_uid(tmp_path) -> None:
    from src.services.facebook_groups.engine import parse_group_uids

    assert parse_group_uids("123456789, https://www.facebook.com/groups/987654321/") == [
        "123456789",
        "987654321",
    ]
    assert parse_group_uids("111111\n222222\n111111") == ["111111", "222222"]
    _store, engine, fake = _engine(tmp_path)
    wave = engine.create_share_wave(
        pages=[{"account_id": "acc-a", "page_id": "page-1", "target_url": "https://www.facebook.com/page-1"}],
        source_url="https://www.facebook.com/other/posts/9",
        text="Vào nhóm này",
        destination="joined",
        group_ids=["555001"],
        cooldown_fixed=0,
    )
    assert wave["created"] == 1
    job = engine.store.share_jobs()[0]
    assert job["group_id"] == "555001"
    assert job["destination"] == "typed"
    engine.run_share_wave(wave["batch_ids"])
    assert fake.publish_calls[0]["group_id"] == "555001"


def test_share_wave_many_pages_one_browser_per_account(tmp_path) -> None:
    store, engine, fake = _engine(tmp_path)
    opened: list[str] = []
    open_accounts: set[str] = set()
    guard = threading.Lock()

    def begin(account_id: str) -> None:
        with guard:
            assert account_id not in open_accounts
            open_accounts.add(account_id)
            opened.append(account_id)

    def end(account_id: str) -> None:
        with guard:
            assert account_id in open_accounts
            open_accounts.remove(account_id)

    fake.begin_account = begin
    fake.end_account = end
    store.upsert_membership(
        {
            "account_id": "acc-a",
            "page_id": "page-1",
            "group_id": "g-ok",
            "membership_status": "JOINED",
            "posting_permission": "UNKNOWN",
        }
    )
    store.upsert_membership(
        {
            "account_id": "acc-a",
            "page_id": "page-1",
            "group_id": "g-deny",
            "membership_status": "JOINED",
            "posting_permission": "DENIED",
        }
    )
    pages = [
        {"account_id": "acc-a", "page_id": "page-1", "target_url": "https://www.facebook.com/page-1"},
        {"account_id": "acc-a", "page_id": "page-2", "target_url": "https://www.facebook.com/page-2"},
        {"account_id": "acc-b", "page_id": "page-3", "target_url": "https://www.facebook.com/page-3"},
    ]
    wave = engine.create_share_wave(
        pages=pages,
        source_url="https://www.facebook.com/other/posts/9",
        text="Cùng một bài",
        destination="both",
        cooldown_fixed=0,
    )
    assert wave["accounts"] == 2
    assert wave["created"] == 4
    assert "g-deny" not in [job.get("group_id") for batch in wave["batches"] for job in store.share_jobs() if job.get("batch_id") == batch["id"]]
    engine.run_share_wave(wave["batch_ids"])
    assert sorted(opened) == ["acc-a", "acc-b"]
    assert open_accounts == set()
    assert len(fake.publish_calls) == 4
    assert {call["page_id"] for call in fake.publish_calls} == {"page-1", "page-2", "page-3"}
    joined_only = engine.create_share_wave(
        pages=pages,
        source_url="https://www.facebook.com/other/posts/10",
        text="Chỉ nhóm",
        destination="joined",
        cooldown_fixed=0,
    )
    assert joined_only["created"] == 1
    assert joined_only["accounts"] == 1


def test_pause_share_outside_accounts_stops_other_tk(tmp_path) -> None:
    """Lọc 1 TK → pause batch của TK khác (wave cũ không chạy ngầm)."""
    _store, engine, _fake = _engine(tmp_path)
    wave_a = engine.create_share_wave(
        pages=[{"account_id": "acc-a", "page_id": "p1", "target_url": "https://www.facebook.com/p1"}],
        source_url="https://www.facebook.com/reel/1",
        text="A",
        destination="page",
        cooldown_fixed=0,
    )
    wave_b = engine.create_share_wave(
        pages=[{"account_id": "acc-b", "page_id": "p2", "target_url": "https://www.facebook.com/p2"}],
        source_url="https://www.facebook.com/reel/2",
        text="B",
        destination="page",
        cooldown_fixed=0,
    )
    bid_a = wave_a["batch_ids"][0]
    bid_b = wave_b["batch_ids"][0]
    assert engine._batch(bid_a)["status"] == "READY"
    assert engine._batch(bid_b)["status"] == "READY"
    stopped = engine.pause_share_outside_accounts({"acc-a"})
    assert "acc-b" in stopped
    assert engine._batch(bid_b)["status"] == "PAUSED"
    assert engine._batch(bid_a)["status"] == "READY"
    assert bid_b in engine._cancel


def test_share_two_batches_same_account_never_concurrent_browser(tmp_path) -> None:
    """Resume nhiều batch cùng TK: chỉ 1 begin tại một thời điểm (không 2 Firefox)."""
    store, engine, fake = _engine(tmp_path)
    open_accounts: set[str] = set()
    peak: dict[str, int] = {"n": 0}
    guard = threading.Lock()
    hold = threading.Event()
    entered = threading.Event()

    def begin(account_id: str) -> None:
        with guard:
            assert account_id not in open_accounts, f"TK {account_id} mở 2 browser cùng lúc"
            open_accounts.add(account_id)
            peak["n"] = max(peak["n"], len(open_accounts))
        entered.set()
        # Giữ browser một lúc để luồng kia cố chen nếu thiếu khóa.
        hold.wait(timeout=0.4)

    def end(account_id: str) -> None:
        with guard:
            open_accounts.discard(account_id)

    fake.begin_account = begin
    fake.end_account = end
    wave1 = engine.create_share_wave(
        pages=[{"account_id": "acc-a", "page_id": "page-1", "target_url": "https://www.facebook.com/p1"}],
        source_url="https://www.facebook.com/reel/111",
        text="A",
        destination="page",
        cooldown_fixed=0,
    )
    wave2 = engine.create_share_wave(
        pages=[{"account_id": "acc-a", "page_id": "page-2", "target_url": "https://www.facebook.com/p2"}],
        source_url="https://www.facebook.com/reel/222",
        text="B",
        destination="page",
        cooldown_fixed=0,
    )
    ids = list(wave1["batch_ids"]) + list(wave2["batch_ids"])
    assert len(ids) == 2

    def _run() -> None:
        engine.run_share_wave(ids)

    # Hai wave song song cùng TK (giống resume + chạy list trùng).
    t1 = threading.Thread(target=_run, daemon=True)
    t2 = threading.Thread(
        target=lambda: (entered.wait(timeout=2), engine.run_share_batch(ids[1])),
        daemon=True,
    )
    t1.start()
    t2.start()
    time.sleep(0.15)
    hold.set()
    t1.join(timeout=10)
    t2.join(timeout=10)
    assert peak["n"] == 1
    assert open_accounts == set()
    assert len(fake.publish_calls) >= 2


def test_share_wave_watches_once_then_shares_with_page_text(tmp_path) -> None:
    _store, engine, fake = _engine(tmp_path)
    order: list[str] = []

    def prepare_source_video(**kwargs):
        order.append(f"watch:{kwargs['watch_seconds']}:{kwargs['comment']}")
        assert kwargs["source_url"].endswith("/videos/9")
        return {"error_code": "", "watched_seconds": kwargs["watch_seconds"], "commented": bool(kwargs["comment"])}

    def publish_link(**kwargs):
        order.append(f"share:{kwargs['page_id']}")
        fake.publish_calls.append(dict(kwargs))
        return {"post_id": f"post-{kwargs['page_id']}", "post_url": kwargs["source_url"]}

    fake.prepare_source_video = prepare_source_video
    fake.publish_link = publish_link
    pages = [
        {"account_id": "acc-a", "page_id": "page-1", "target_url": "https://www.facebook.com/page-1"},
        {"account_id": "acc-a", "page_id": "page-2", "target_url": "https://www.facebook.com/page-2"},
    ]
    wave = engine.create_share_wave(
        pages=pages,
        source_url="https://www.facebook.com/other/videos/9",
        text="Đăng lên page của mình",
        destination="page",
        watch_seconds=12,
        comment="Hay quá",
        cooldown_fixed=0,
    )
    engine.run_share_wave(wave["batch_ids"])
    assert order == [
        "watch:12:Hay quá",
        "share:page-1",
        "watch:12:",
        "share:page-2",
    ]
    assert fake.sleeps == []
    assert fake.publish_calls[0]["text"] == "Đăng lên page của mình"
    assert engine.store.share_batches()[0]["source_prepared"] is True


def test_same_account_waits_longer_before_the_next_page(tmp_path) -> None:
    store, engine, fake = _engine(tmp_path)
    store.upsert_membership(
        {
            "account_id": "acc-a",
            "page_id": "page-1",
            "group_id": "g-ok",
            "membership_status": "JOINED",
            "posting_permission": "UNKNOWN",
        }
    )
    wave = engine.create_share_wave(
        pages=[
            {"account_id": "acc-a", "page_id": "page-1", "target_url": "https://www.facebook.com/page-1"},
            {"account_id": "acc-a", "page_id": "page-2", "target_url": "https://www.facebook.com/page-2"},
        ],
        source_url="https://www.facebook.com/other/posts/9",
        text="Lên page",
        destination="both",
        cooldown_fixed=10,
    )
    engine.run_share_wave(wave["batch_ids"])
    assert len(fake.publish_calls) == 3
    assert fake.sleeps == [10, 20]


def test_two_accounts_share_one_page_each_then_their_next_page(tmp_path) -> None:
    _store, engine, fake = _engine(tmp_path)
    lock = threading.Lock()
    inflight: list[str] = []
    order: dict[str, list[str]] = {"acc-a": [], "acc-b": []}
    overlapped = {"ok": False}

    def publish_link(**kwargs):
        account_id = str(kwargs["account_id"])
        page_id = str(kwargs["page_id"])
        with lock:
            assert account_id not in inflight
            inflight.append(account_id)
            if len(inflight) == 2:
                overlapped["ok"] = True
            order[account_id].append(page_id)
        time.sleep(0.05)
        with lock:
            inflight.remove(account_id)
        fake.publish_calls.append(dict(kwargs))
        return {"post_id": f"post-{page_id}", "post_url": kwargs["source_url"]}

    fake.publish_link = publish_link
    notes: list[str] = []
    engine.on_progress = notes.append
    wave = engine.create_share_wave(
        pages=[
            {"account_id": "acc-a", "account_name": "Minh Tuyết", "page_id": "a1", "page_name": "Page A1", "target_url": "https://www.facebook.com/a1"},
            {"account_id": "acc-a", "account_name": "Minh Tuyết", "page_id": "a2", "page_name": "Page A2", "target_url": "https://www.facebook.com/a2"},
            {"account_id": "acc-b", "account_name": "RicardoInfor", "page_id": "b1", "page_name": "Page B1", "target_url": "https://www.facebook.com/b1"},
            {"account_id": "acc-b", "account_name": "RicardoInfor", "page_id": "b2", "page_name": "Page B2", "target_url": "https://www.facebook.com/b2"},
        ],
        source_url="https://www.facebook.com/reel/9",
        text="Lên page",
        destination="page",
        cooldown_fixed=0,
    )
    engine.run_share_wave(wave["batch_ids"])
    assert overlapped["ok"] is True
    assert order["acc-a"] == ["a1", "a2"]
    assert order["acc-b"] == ["b1", "b2"]
    assert inflight == []
    together = [line for line in notes if "Minh Tuyết" in line and "RicardoInfor" in line]
    assert together
    assert any("Share now" in line for line in together)


def test_second_wave_waits_when_account_still_delaying(tmp_path) -> None:
    """
    Batch DELAYING vẫn chiếm tài khoản — video sau không bỏ lượt (READY mãi).

    Trước đây chỉ nhìn job ACTIVE nên lúc nghỉ giữa bài tài khoản bị coi là rảnh,
    video mới chạy 1 TK còn TK kia bỏ qua.
    """
    _store, engine, fake = _engine(tmp_path)
    gate = threading.Event()
    started_second = threading.Event()
    order: list[str] = []

    def publish_link(**kwargs):
        account_id = str(kwargs["account_id"])
        page_id = str(kwargs["page_id"])
        order.append(f"{account_id}:{page_id}")
        fake.publish_calls.append(dict(kwargs))
        if account_id == "acc-a" and page_id == "a1":
            # Giữ batch 1 ở DELAYING đủ lâu để wave 2 thử chiếm cùng TK.
            gate.wait(timeout=2.0)
        return {"post_id": f"post-{page_id}", "post_url": kwargs["source_url"]}

    fake.publish_link = publish_link
    wave1 = engine.create_share_wave(
        pages=[
            {"account_id": "acc-a", "account_name": "Andy", "page_id": "a1", "page_name": "A1", "target_url": "https://www.facebook.com/a1"},
            {"account_id": "acc-b", "account_name": "Minh", "page_id": "b1", "page_name": "B1", "target_url": "https://www.facebook.com/b1"},
        ],
        source_url="https://www.facebook.com/reel/111",
        text="Video 1",
        destination="page",
        cooldown_fixed=1,
    )
    wave2 = engine.create_share_wave(
        pages=[
            {"account_id": "acc-a", "account_name": "Andy", "page_id": "a2", "page_name": "A2", "target_url": "https://www.facebook.com/a2"},
            {"account_id": "acc-b", "account_name": "Minh", "page_id": "b2", "page_name": "B2", "target_url": "https://www.facebook.com/b2"},
        ],
        source_url="https://www.facebook.com/reel/222",
        text="Video 2",
        destination="page",
        cooldown_fixed=0,
    )

    def _run_first() -> None:
        engine.run_share_wave(wave1["batch_ids"])

    def _run_second() -> None:
        started_second.set()
        engine.run_share_wave(wave2["batch_ids"])

    t1 = threading.Thread(target=_run_first, daemon=True)
    t2 = threading.Thread(target=_run_second, daemon=True)
    t1.start()
    # Đợi Andy bắt đầu bài đầu (batch đang chạy / sắp DELAYING).
    for _ in range(50):
        if any(x.startswith("acc-a:a1") for x in order):
            break
        time.sleep(0.02)
    t2.start()
    assert started_second.wait(timeout=2.0)
    # Wave 2 đã vào nhưng acc-a chưa được share a2 khi a1 còn giữ chỗ.
    time.sleep(0.2)
    assert "acc-a:a2" not in order
    gate.set()
    t1.join(timeout=5.0)
    t2.join(timeout=5.0)
    assert "acc-a:a1" in order
    assert "acc-a:a2" in order
    assert "acc-b:b1" in order
    assert "acc-b:b2" in order
    # Cả hai batch video 2 phải hoàn tất — không còn READY bỏ sót.
    for batch_id in wave2["batch_ids"]:
        batch = next(b for b in engine.store.share_batches() if str(b.get("id")) == batch_id)
        assert batch.get("status") == "COMPLETED"


def test_share_switches_to_each_page_before_reel_watch_and_share(tmp_path) -> None:
    _store, engine, fake = _engine(tmp_path)
    order: list[str] = []

    def switch_to_page(**kwargs):
        if kwargs["page_id"] == "page-bad":
            return {"error_code": "TEMPORARY_ERROR", "error_message": "Chưa chuyển được sang Page"}
        order.append(f"switch:{kwargs['page_id']}")
        return {"error_code": ""}

    def browse_reels(**kwargs):
        order.append(f"reel:{kwargs['reel_seconds']}")
        return {"error_code": "", "browsed_seconds": kwargs["reel_seconds"]}

    def prepare_source_video(**kwargs):
        parts = [f"watch:{kwargs['watch_seconds']}"]
        if kwargs.get("like"):
            parts.append("like")
        if kwargs.get("comment"):
            parts.append("comment")
        order.append("+".join(parts))
        return {
            "error_code": "",
            "watched_seconds": kwargs["watch_seconds"],
            "commented": bool(kwargs["comment"]),
            "liked": bool(kwargs.get("like")),
        }

    def like_source_video(**kwargs):
        order.append("like-riêng")
        return {"error_code": "", "liked": True, "source_url": kwargs["source_url"]}

    def publish_link(**kwargs):
        order.append(f"share:{kwargs['page_id']}")
        fake.publish_calls.append(dict(kwargs))
        return {"post_id": f"post-{kwargs['page_id']}", "post_url": kwargs["source_url"]}

    fake.switch_to_page = switch_to_page
    fake.browse_reels = browse_reels
    fake.prepare_source_video = prepare_source_video
    fake.like_source_video = like_source_video
    fake.publish_link = publish_link
    wave = engine.create_share_wave(
        pages=[
            {"account_id": "acc-a", "page_id": "page-1", "page_name": "Một", "target_url": "https://www.facebook.com/page-1"},
            {"account_id": "acc-a", "page_id": "page-bad", "page_name": "Lỗi", "target_url": "https://www.facebook.com/page-bad"},
            {"account_id": "acc-a", "page_id": "page-2", "page_name": "Hai", "target_url": "https://www.facebook.com/page-2"},
        ],
        source_url="https://www.facebook.com/reel/9",
        text="Lên page",
        destination="page",
        reel_seconds=5,
        watch_seconds=4,
        comment="Hay",
        like_video=True,
        cooldown_fixed=0,
    )
    engine.run_share_wave(wave["batch_ids"])
    # Like/comment phân bổ thưa — chỉ kiểm tra thứ tự chuyển Page → xem → chia sẻ.
    assert [step for step in order if step.startswith("switch:") or step.startswith("share:")] == [
        "switch:page-1",
        "share:page-1",
        "switch:page-2",
        "share:page-2",
    ]
    assert order.count("reel:5") == 2
    assert any(step.startswith("watch:4") for step in order)
    assert [call["page_id"] for call in fake.publish_calls] == ["page-1", "page-2"]
    assert all(call["like_video"] is False for call in fake.publish_calls)


def test_share_continues_to_the_next_page_when_one_page_crashes(tmp_path) -> None:
    _store, engine, fake = _engine(tmp_path)
    seen: list[str] = []

    def switch_to_page(**kwargs):
        seen.append(kwargs["page_id"])
        if kwargs["page_id"] == "page-bad":
            raise RuntimeError("trình duyệt đứng")
        return {"error_code": ""}

    def publish_link(**kwargs):
        fake.publish_calls.append(dict(kwargs))
        return {"post_id": f"post-{kwargs['page_id']}", "post_url": kwargs["source_url"]}

    fake.switch_to_page = switch_to_page
    fake.publish_link = publish_link
    wave = engine.create_share_wave(
        pages=[
            {"account_id": "acc-a", "page_id": "page-bad", "page_name": "Lỗi", "target_url": "https://www.facebook.com/page-bad"},
            {"account_id": "acc-a", "page_id": "page-2", "page_name": "Hai", "target_url": "https://www.facebook.com/page-2"},
        ],
        source_url="https://www.facebook.com/reel/9",
        text="",
        destination="page",
        reel_seconds=0,
        watch_seconds=0,
        comment="",
        like_video=False,
        cooldown_fixed=0,
    )
    engine.run_share_wave(wave["batch_ids"])
    assert seen == ["page-bad", "page-2"]
    assert [call["page_id"] for call in fake.publish_calls] == ["page-2"]


def test_share_wave_likes_sparse_pages_not_every_share(tmp_path) -> None:
    """13 Page chia sẻ → chỉ ~2–3 Page được Like (không Like hết)."""
    _store, engine, fake = _engine(tmp_path)
    prepare_likes: list[bool] = []

    def prepare_source_video(**kwargs):
        prepare_likes.append(bool(kwargs.get("like")))
        return {
            "error_code": "",
            "watched_seconds": kwargs.get("watch_seconds") or 0,
            "liked": bool(kwargs.get("like")),
            "commented": False,
        }

    fake.prepare_source_video = prepare_source_video
    fake.publish_link = lambda **kwargs: {
        "post_id": f"post-{kwargs['page_id']}",
        "post_url": kwargs["source_url"],
    }
    pages = [
        {
            "account_id": "acc-a",
            "page_id": f"page-{i}",
            "target_url": f"https://www.facebook.com/page-{i}",
        }
        for i in range(1, 14)
    ]
    wave = engine.create_share_wave(
        pages=pages,
        source_url="https://www.facebook.com/reel/9",
        text="Lên page",
        destination="page",
        like_video=True,
        engage_mode="like",
        cooldown_fixed=0,
    )
    batch = engine.store.share_batches()[0]
    like_ids = [str(x) for x in (batch.get("like_page_ids") or [])]
    assert 2 <= len(like_ids) <= 3
    engine.run_share_wave(wave["batch_ids"])
    # Không xem video (0s) → chỉ Page được chọn Like mới gọi prepare.
    assert 2 <= len(prepare_likes) <= 3
    assert all(prepare_likes)


def test_share_wave_comments_again_when_pages_draw_different_lines(tmp_path) -> None:
    """Page được chọn bình luận dùng câu khác nhau; phần còn lại chỉ chia sẻ."""
    store = GroupStore(tmp_path / "facebook_groups.json")
    fake = FakeGroups()
    engine = GroupEngine(store, fake, rng=random.Random(1))
    comments: list[str] = []

    def prepare_source_video(**kwargs):
        comments.append(str(kwargs.get("comment") or ""))
        return {"error_code": "", "watched_seconds": kwargs["watch_seconds"], "commented": bool(kwargs["comment"])}

    fake.prepare_source_video = prepare_source_video
    fake.publish_link = lambda **kwargs: {"post_id": f"post-{kwargs['page_id']}", "post_url": kwargs["source_url"]}
    pages = [
        {
            "account_id": "acc-a",
            "page_id": f"page-{i}",
            "target_url": f"https://www.facebook.com/page-{i}",
        }
        for i in range(1, 14)
    ]
    engine.create_share_wave(
        pages=pages,
        source_url="https://www.facebook.com/other/videos/9",
        text="Lên tường",
        destination="page",
        watch_seconds=8,
        comment="Câu một\nCâu hai\nCâu ba\nCâu bốn",
        engage_mode="comment",
        cooldown_fixed=0,
    )
    engine.run_share_wave([str(row.get("id") or "") for row in engine.store.share_batches()])
    posted = [item for item in comments if item]
    assert 3 <= len(posted) <= 4
    assert len(comments) == 13
    assert sum(1 for c in comments if not c) >= 9
    assert len(set(posted)) >= 2


def test_share_wave_browses_reels_then_watches_each_page(tmp_path) -> None:
    store = GroupStore(tmp_path / "facebook_groups.json")
    fake = FakeGroups()
    engine = GroupEngine(store, fake, rng=random.Random(4))
    order: list[str] = []

    def browse_reels(**kwargs):
        order.append(f"reel:{kwargs['reel_seconds']}")
        return {"error_code": "", "browsed_seconds": kwargs["reel_seconds"]}

    def prepare_source_video(**kwargs):
        order.append(f"watch:{kwargs['watch_seconds']}")
        return {"error_code": "", "watched_seconds": kwargs["watch_seconds"], "commented": False}

    def publish_link(**kwargs):
        order.append(f"share:{kwargs['page_id']}:{kwargs['group_id']}")
        fake.publish_calls.append(dict(kwargs))
        return {"post_id": "post-1", "post_url": kwargs["source_url"]}

    fake.browse_reels = browse_reels
    fake.prepare_source_video = prepare_source_video
    fake.publish_link = publish_link
    store.upsert_membership(
        {
            "account_id": "acc-a",
            "page_id": "page-1",
            "group_id": "g-ok",
            "membership_status": "JOINED",
            "posting_permission": "UNKNOWN",
        }
    )
    wave = engine.create_share_wave(
        pages=[
            {"account_id": "acc-a", "page_id": "page-1", "target_url": "https://www.facebook.com/page-1"},
            {"account_id": "acc-a", "page_id": "page-2", "target_url": "https://www.facebook.com/page-2"},
        ],
        source_url="https://www.facebook.com/other/videos/9",
        text="Lên tường",
        destination="both",
        watch_seconds=10,
        watch_max_seconds=40,
        reel_seconds=15,
        cooldown_fixed=0,
    )
    jobs = [job for job in store.share_jobs() if job.get("batch_id") == wave["batch_ids"][0]]
    by_page: dict[str, set[int]] = {}
    for job in jobs:
        by_page.setdefault(str(job["page_id"]), set()).add(int(job["watch_seconds"]))
    assert set(by_page) == {"page-1", "page-2"}
    for seconds in by_page.values():
        assert len(seconds) == 1
        assert 10 <= next(iter(seconds)) <= 40
    engine.run_share_wave(wave["batch_ids"])
    assert order[0] == "reel:15"
    assert order[1].startswith("watch:")
    assert "share:page-1:" in order
    assert "share:page-1:g-ok" in order
    assert order.index("share:page-1:") > order.index(order[1])
    assert sum(item.startswith("watch:") for item in order) == 2
    assert sum(item.startswith("reel:") for item in order) == 2
    assert order.index("reel:15", 1) > order.index("share:page-1:")
    assert engine.store.share_batches()[0]["reels_browsed"] is True


def test_share_wave_comments_then_shares_to_page_and_group(tmp_path) -> None:
    store, engine, fake = _engine(tmp_path)
    order: list[str] = []
    store.upsert_membership(
        {
            "account_id": "acc-a",
            "page_id": "page-1",
            "group_id": "g-ok",
            "membership_status": "JOINED",
            "posting_permission": "UNKNOWN",
        }
    )

    def prepare_source_video(**kwargs):
        order.append(f"prep:{kwargs['watch_seconds']}:{kwargs['comment']}")
        return {"error_code": "", "watched_seconds": kwargs["watch_seconds"], "commented": bool(kwargs["comment"])}

    def publish_link(**kwargs):
        order.append(kwargs["group_id"] or "page")
        fake.publish_calls.append(dict(kwargs))
        return {"post_id": "post-1", "post_url": kwargs["source_url"]}

    fake.prepare_source_video = prepare_source_video
    fake.publish_link = publish_link
    wave = engine.create_share_wave(
        pages=[{"account_id": "acc-a", "page_id": "page-1", "target_url": "https://www.facebook.com/page-1"}],
        source_url="https://www.facebook.com/other/videos/9",
        text="Lên tường",
        destination="both",
        watch_seconds=15,
        comment="Dưới video",
        cooldown_fixed=0,
    )
    engine.run_share_wave(wave["batch_ids"])
    assert order[0] == "prep:15:Dưới video"
    assert [item for item in order if item.startswith("prep:")] == ["prep:15:Dưới video"]
    assert "page" in order
    assert "g-ok" in order
    assert {call["text"] for call in fake.publish_calls} == {"Lên tường"}


def test_share_wave_still_shares_when_comment_box_is_missing(tmp_path) -> None:
    _store, engine, fake = _engine(tmp_path)

    def prepare_source_video(**kwargs):
        del kwargs
        return {"error_code": "", "watched_seconds": 10, "commented": False, "error_message": "Chưa thấy ô bình luận"}

    fake.prepare_source_video = prepare_source_video
    wave = engine.create_share_wave(
        pages=[{"account_id": "acc-a", "page_id": "page-1", "target_url": "https://www.facebook.com/page-1"}],
        source_url="https://www.facebook.com/other/videos/9",
        text="Lên tường",
        destination="page",
        watch_seconds=10,
        comment="Dưới video",
        cooldown_fixed=0,
    )
    engine.run_share_wave(wave["batch_ids"])
    assert len(fake.publish_calls) == 1
    batch = engine.store.share_batches()[0]
    assert batch["source_watched"] is True
    assert batch["source_prepared"] is True
    assert batch["status"] == "COMPLETED"
    assert "bình luận" in str(batch.get("comment_warning") or "").casefold()


def test_share_wave_gives_each_account_a_different_comment(tmp_path) -> None:
    """Nhiều account / Page: chỉ một phần Page có comment; các câu không trùng khi đủ pool."""
    store = GroupStore(tmp_path / "facebook_groups.json")
    engine = GroupEngine(store, FakeGroups(), rng=random.Random(1))
    pages = []
    for i in range(1, 14):
        pages.append(
            {
                "account_id": "acc-a" if i <= 7 else "acc-b",
                "page_id": f"page-{i}",
                "target_url": f"https://www.facebook.com/page-{i}",
            }
        )
    wave = engine.create_share_wave(
        pages=pages,
        source_url="https://www.facebook.com/other/videos/9",
        text="Lên tường",
        destination="page",
        watch_seconds=10,
        comment="Câu một\nCâu hai\nCâu ba\nCâu bốn",
        engage_mode="comment",
        cooldown_fixed=0,
    )
    assert wave["accounts"] == 2
    job_comments = [str(job.get("comment") or "") for job in engine.store.share_jobs()]
    posted = [c for c in job_comments if c]
    assert 3 <= len(posted) <= 4
    assert len(job_comments) == 13
    assert len(set(posted)) >= 2


def test_share_wave_gives_each_page_a_different_comment(tmp_path) -> None:
    store = GroupStore(tmp_path / "facebook_groups.json")
    engine = GroupEngine(store, FakeGroups(), rng=random.Random(1))
    pages = [
        {
            "account_id": "acc-a",
            "page_id": f"page-{i}",
            "page_name": f"Page {i}",
            "target_url": f"https://www.facebook.com/page-{i}",
        }
        for i in range(1, 14)
    ]
    wave = engine.create_share_wave(
        pages=pages,
        source_url="https://www.facebook.com/other/videos/9",
        text="Lên tường",
        destination="page",
        watch_seconds=10,
        comment="Câu một\nCâu hai\nCâu ba\nCâu bốn",
        engage_mode="comment",
        cooldown_fixed=0,
    )
    assert wave["created"] == 13
    comments = {str(job.get("page_id")): str(job.get("comment") or "") for job in engine.store.share_jobs()}
    posted = {pid: text for pid, text in comments.items() if text}
    assert 3 <= len(posted) <= 4
    assert len(set(posted.values())) >= 2
    assert sum(1 for text in comments.values() if not text) >= 9


def test_waiting_links_round_trip() -> None:
    from datetime import datetime

    from src.services.facebook_groups.share_schedule import (
        PlannedShareLink,
        queue_from_settings,
        queue_to_settings,
    )

    when = datetime(2026, 10, 16, 21, 30)
    saved = queue_to_settings(
        [
            PlannedShareLink(when=when, url="https://www.facebook.com/reel/9", label="21:30", state="waiting"),
            PlannedShareLink(when=when, url="https://www.facebook.com/reel/8", label="Ngay", state="done"),
        ]
    )
    assert len(saved) == 1
    restored = queue_from_settings(saved)
    assert restored[0].url.endswith("/reel/9")
    assert restored[0].state == "waiting"
    assert restored[0].when == when


def test_share_settings_keep_comments_and_last_batches(tmp_path) -> None:
    store = GroupStore(tmp_path / "facebook_groups.json")
    store.save_share_settings({"comments": "Hay quá\nXem hay", "like_video": True, "last_batch_ids": ["b1"]})
    store.save_share_settings({"caption": "Lên page"})
    data = store.share_settings()
    assert data["comments"] == "Hay quá\nXem hay"
    assert data["caption"] == "Lên page"
    assert data["last_batch_ids"] == ["b1"]
    assert data["like_video"] is True


def test_share_settings_keep_receiving_page_ticks(tmp_path) -> None:
    """Tick Page nhận (checked/targets) phải được lưu lại khi mở lại app."""
    store = GroupStore(tmp_path / "facebook_groups.json")
    store.save_share_settings(
        {
            "watch": {
                "sources": "https://www.facebook.com/src",
                "enabled": False,
                "targets": ["page_a", "page_b"],
                "checked": ["page_a", "page_b", "page_c"],
            }
        }
    )
    data = store.share_settings()
    watch = data["watch"]
    assert watch["targets"] == ["page_a", "page_b"]
    assert watch["checked"] == ["page_a", "page_b", "page_c"]


def test_share_wave_skips_watch_when_seconds_and_comment_empty(tmp_path) -> None:
    _store, engine, fake = _engine(tmp_path)
    fake.prepare_calls = []
    fake.prepare_source_video = lambda **kwargs: fake.prepare_calls.append(kwargs)
    wave = engine.create_share_wave(
        pages=[{"account_id": "acc-a", "page_id": "page-1", "target_url": "https://www.facebook.com/page-1"}],
        source_url="https://www.facebook.com/other/posts/9",
        text="Chỉ chia sẻ",
        destination="page",
        cooldown_fixed=0,
    )
    engine.run_share_wave(wave["batch_ids"])
    assert fake.prepare_calls == []
    assert len(fake.publish_calls) == 1


def test_discovery_source_has_no_maximum_member_field() -> None:
    from pathlib import Path

    banned = ("max_members", "maximum_members", "member_count_max", "MAX_MEMBERS")
    roots = [Path("src/services/facebook_groups"), Path("src/gui/group_manager_dialog.py")]
    files = []
    for root in roots:
        files.extend(root.rglob("*.py") if root.is_dir() else [root])
    blob = "\n".join(path.read_text(encoding="utf-8") for path in files)
    for name in banned:
        assert name not in blob


class _SwitchPage:
    """Trang giả cho luồng chuyển Page của provider."""

    def __init__(self) -> None:
        self.url = "https://www.facebook.com/reel/9"

    def wait_for_timeout(self, millis: int) -> None:
        del millis


def _patch_switch(monkeypatch, results: list[bool]) -> list[str]:
    """Thay các bước Facebook thật bằng bản ghi lại thứ tự gọi."""
    import src.automation.facebook_actions as fa

    calls: list[str] = []

    def navigate(page, url):
        calls.append(f"goto:{url}")
        page.url = url

    def robust(page, *, page_display_name="", page_url=""):
        calls.append("switch")
        return results.pop(0)

    monkeypatch.setattr(fa, "navigate_to_url", navigate)
    monkeypatch.setattr(fa, "_robust_switch_to_target_page", robust)
    monkeypatch.setattr(fa, "_target_page_role_satisfied", lambda *a, **k: True)
    monkeypatch.setattr(fa, "_go_facebook_personal_home", lambda page: calls.append("home") or True)
    monkeypatch.setattr(fa, "_dismiss_open_profile_menu", lambda page: None)
    monkeypatch.setattr(fa, "_disable_view_only_guard", lambda page: None)
    monkeypatch.setattr(fa, "_enable_view_only_guard", lambda page, force=False: None)
    return calls


def test_switch_opens_target_page_first_and_retries_once(monkeypatch) -> None:
    """Mở Page đích rồi mới switch; lần 1 hụt thì về trang chủ và thử lần 2."""
    provider = FacebookGroupProvider()
    provider._page = _SwitchPage()
    provider._account_id = "acc-a"
    calls = _patch_switch(monkeypatch, [False, True])
    result = provider.switch_to_page(
        account_id="acc-a", page_id="111", page_name="Một", page_url="https://www.facebook.com/111"
    )
    assert result["error_code"] == ""
    assert calls == [
        "goto:https://www.facebook.com/111",
        "switch",
        "home",
        "goto:https://www.facebook.com/111",
        "switch",
    ]


def test_switch_failure_leaves_browser_on_clean_home(monkeypatch) -> None:
    """Ba lần đều hụt: báo lỗi và về trang chủ để Page sau không bị kẹt theo."""
    provider = FacebookGroupProvider()
    provider._page = _SwitchPage()
    provider._account_id = "acc-a"
    calls = _patch_switch(monkeypatch, [False, False, False])
    result = provider.switch_to_page(
        account_id="acc-a", page_id="111", page_name="Một", page_url="https://www.facebook.com/111"
    )
    assert result["error_code"] == "TEMPORARY_ERROR"
    assert "đã thử 3 lần" in str(result.get("error_message") or "")
    assert calls.count("switch") == 3
    assert calls[-1] == "home"
