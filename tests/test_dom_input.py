"""Click/cuộn DOM không dùng page.mouse."""

from __future__ import annotations

from types import SimpleNamespace

from src.utils.dom_input import (
    CHROMIUM_BACKGROUND_ARGS,
    chromium_background_launch_kwargs,
    click_locator_dom,
    click_point_dom,
    scroll_by_dom,
)


def test_click_locator_dom_evaluates_click() -> None:
    calls: list[str] = []

    class _Loc:
        def wait_for(self, **_k) -> None:
            return None

        def evaluate(self, script: str) -> bool:
            calls.append(script)
            return True

        @property
        def first(self):
            return self

    assert click_locator_dom(_Loc()) is True
    assert calls and "el.click" in calls[0]


def test_click_point_dom_and_scroll() -> None:
    page = SimpleNamespace(evaluate=lambda *_a, **_k: True)
    assert click_point_dom(page, 10, 20) is True
    assert scroll_by_dom(page, 400) is True
    assert click_point_dom(None, 1, 1) is False


def test_chromium_background_kwargs() -> None:
    kw = chromium_background_launch_kwargs(headless=True)
    assert kw["headless"] is True
    assert "--mute-audio" in kw["args"]
    assert all(a in CHROMIUM_BACKGROUND_ARGS for a in kw["args"])
