"""Tests thời lượng / giới hạn module tương tác."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.services.human_interaction_modules import run_shuffled_interaction_modules
from src.services.human_interaction_profile import PROFILES, resolve_profile


def test_normal_profile_shorter_than_before() -> None:
    normal = PROFILES["normal"]
    assert normal.scroll_rounds_max <= 8
    assert normal.deep_delay_max_sec <= 6.0
    assert normal.max_modules_per_run == 2
    assert normal.reels_clip_max_ms <= 6000
    assert normal.max_worker_sec <= 300.0
    assert normal.max_module_phase_sec <= 150.0


def test_locate_like_uses_tagged_button() -> None:
    from unittest.mock import MagicMock

    article = MagicMock()
    like_btn = MagicMock()
    like_btn.is_visible.return_value = True
    like_btn.get_attribute.return_value = "false"
    article.page.locator.return_value.first = like_btn

    from src.services.human_interaction_modules import _locate_like_in_article

    found = _locate_like_in_article(article)
    assert found is like_btn
    article.page.locator.assert_called_with('[data-toolfb-like-btn="1"]')


def test_article_viewport_requires_tag() -> None:
    from unittest.mock import MagicMock, patch

    page = MagicMock()
    loc = MagicMock()
    loc.is_visible.return_value = True
    page.locator.return_value.first = loc

    from src.services.human_interaction_modules import _article_most_visible_in_viewport

    with patch(
        "src.services.human_interaction_modules._tag_visible_post_like_target",
        return_value=False,
    ):
        assert _article_most_visible_in_viewport(page) is None

    with patch(
        "src.services.human_interaction_modules._tag_visible_post_like_target",
        return_value=True,
    ):
        assert _article_most_visible_in_viewport(page) is loc
    page.locator.assert_called_with('[data-toolfb-feed-article="1"]')


def test_run_shuffled_caps_modules_per_run() -> None:
    page = MagicMock()
    page.is_closed.return_value = False
    cfg = resolve_profile("fast")

    def _always_ok(_page, *, probability=1.0, cfg=None, should_stop=None):  # noqa: ANN001, ARG001
        return True

    with patch("src.services.human_interaction_modules.module_newsfeed_like", side_effect=_always_ok) as m1, patch(
        "src.services.human_interaction_modules.module_search_reels",
        side_effect=_always_ok,
    ) as m2, patch(
        "src.services.human_interaction_modules.module_reels_watch",
        side_effect=_always_ok,
    ) as m3, patch(
        "src.services.human_interaction_modules.module_search_fanpage",
        side_effect=_always_ok,
    ) as m4, patch(
        "src.services.human_interaction_modules.module_post_story",
        side_effect=_always_ok,
    ) as m5, patch(
        "src.services.human_interaction_modules.deep_delay_between_modules",
    ) as deep, patch(
        "src.services.human_interaction_modules._module_micro_pause",
        return_value=True,
    ), patch(
        "src.services.human_interaction_modules.random.shuffle",
        side_effect=lambda xs: xs,
    ):
        run_shuffled_interaction_modules(page, profile=cfg)
        total = m1.call_count + m2.call_count + m3.call_count + m4.call_count + m5.call_count
        assert total == cfg.max_modules_per_run
        assert deep.call_count == cfg.max_modules_per_run - 1
