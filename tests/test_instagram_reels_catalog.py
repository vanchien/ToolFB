"""Unit test cho chuẩn hoá URL và trích link reel Instagram."""

from __future__ import annotations

from src.services.instagram_reels_catalog import (
    _extract_reel_urls_from_hrefs,
    _extract_reel_urls_from_text,
    is_instagram_reels_tab_url,
    normalize_instagram_reels_tab_url,
    scan_instagram_profile_reels_page,
)


def test_normalize_profile_to_reels_tab() -> None:
    url = "https://www.instagram.com/cook_with_tabu/"
    assert normalize_instagram_reels_tab_url(url) == "https://www.instagram.com/cook_with_tabu/reels/"


def test_normalize_reels_tab_unchanged() -> None:
    url = "https://www.instagram.com/cook_with_tabu/reels/"
    assert normalize_instagram_reels_tab_url(url) == url


def test_is_instagram_reels_tab_url() -> None:
    assert is_instagram_reels_tab_url("https://www.instagram.com/user/reels/")
    assert is_instagram_reels_tab_url("https://www.instagram.com/user/")
    assert not is_instagram_reels_tab_url("https://www.instagram.com/reel/ABC/")


def test_extract_reel_urls_from_hrefs() -> None:
    hrefs = [
        "/reel/AbCdEfGhIj/",
        "https://www.instagram.com/reel/XyZ_12-34/",
        "https://www.instagram.com/p/photo_only/",
    ]
    urls = _extract_reel_urls_from_hrefs(hrefs)
    assert urls == [
        "https://www.instagram.com/reel/AbCdEfGhIj/",
        "https://www.instagram.com/reel/XyZ_12-34/",
    ]


def test_extract_reel_urls_from_text() -> None:
    text = 'href="https://www.instagram.com/reel/ShortCode1/" other'
    urls = _extract_reel_urls_from_text(text)
    assert urls == ["https://www.instagram.com/reel/ShortCode1/"]


def test_login_wall_check_does_not_raise_on_reels_url(monkeypatch) -> None:
    """Regression: ``"login" in path.endswith(...)`` từng gây TypeError với bool."""

    class _FakePage:
        url = "https://www.instagram.com/cook_with_tabu/reels/"

        def inner_text(self, *_a, **_k) -> str:
            return "cook_with_tabu reels"

        def eval_on_selector_all(self, *_a, **_k) -> list[str]:
            return ["/reel/AbCdEfGhIj/"]

        def content(self) -> str:
            return ""

        def goto(self, *_a, **_k) -> None:
            return None

        def evaluate(self, *_a, **_k) -> None:
            return None

        @property
        def mouse(self):
            class _M:
                def wheel(self, *_a, **_k) -> None:
                    return None

            return _M()

    class _FakeContext:
        def new_page(self) -> _FakePage:
            return _FakePage()

        def close(self) -> None:
            return None

    class _FakeBrowser:
        def new_context(self, **_k) -> _FakeContext:
            return _FakeContext()

        def close(self) -> None:
            return None

    class _FakeChromium:
        def launch(self, **_k) -> _FakeBrowser:
            return _FakeBrowser()

    class _FakePW:
        chromium = _FakeChromium()

        def __enter__(self):
            return self

        def __exit__(self, *_a):
            return False

    monkeypatch.setattr(
        "src.services.instagram_reels_catalog.sync_playwright",
        lambda: _FakePW(),
    )
    monkeypatch.setattr("src.services.instagram_reels_catalog.time.sleep", lambda *_a: None)
    monkeypatch.setattr(
        "src.services.instagram_reels_catalog.Stealth",
        lambda: type("S", (), {"apply_stealth_sync": lambda self, page: None})(),
    )

    res = scan_instagram_profile_reels_page(
        page_url="https://www.instagram.com/cook_with_tabu/reels/",
        max_scroll_rounds=5,
        scroll_until_end=False,
    )
    assert res["ok"] is True
    assert len(res["items"]) == 1
