"""Trình duyệt đóng giữa wave — phải mở lại, không fail cả list Page."""

from __future__ import annotations

from src.services.facebook_groups.facebook_provider import FacebookGroupProvider


class _DeadPage:
    @property
    def url(self) -> str:
        raise RuntimeError("Target page, context or browser has been closed")


def test_begin_account_reopens_when_page_object_is_dead(monkeypatch) -> None:
    provider = FacebookGroupProvider()
    provider._account_id = "acc-a"
    provider._page = _DeadPage()  # type: ignore[assignment]
    opened: list[str] = []

    def _open(account_id: str) -> None:
        opened.append(account_id)
        provider._page = type("Alive", (), {"url": "https://www.facebook.com/"})()  # type: ignore[assignment]
        provider._account_id = account_id

    monkeypatch.setattr(provider, "_open_browser", _open)
    monkeypatch.setattr(provider, "end_account", lambda _aid, **_k: None)
    provider.begin_account("acc-a")
    assert opened == ["acc-a"]
    assert provider._page_is_alive() is True


def test_ensure_browser_recovers_closed_page(monkeypatch) -> None:
    provider = FacebookGroupProvider()
    provider._account_id = "acc-a"
    provider._page = _DeadPage()  # type: ignore[assignment]
    opened: list[str] = []

    def _open(account_id: str) -> None:
        opened.append(account_id)
        provider._page = type("Alive", (), {"url": "https://www.facebook.com/"})()  # type: ignore[assignment]
        provider._account_id = account_id

    monkeypatch.setattr(provider, "_open_browser", _open)
    monkeypatch.setattr(provider, "end_account", lambda _aid, **_k: None)
    assert provider.ensure_browser("acc-a") is True
    assert opened == ["acc-a"]
