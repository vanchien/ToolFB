"""Tests Playwright thread-affinity cho worker tương tác."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.automation.browser_factory import prepare_playwright_sync_thread, sync_close_persistent_context


def test_prepare_playwright_sync_thread_clears_closed_loop() -> None:
    import asyncio

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.close()
    prepare_playwright_sync_thread(label="test")
    fresh = asyncio.get_event_loop()
    assert fresh is not loop
    assert not fresh.is_closed()


def test_prepare_playwright_sync_thread_replaces_running_loop() -> None:
    import asyncio

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    async def _noop() -> None:
        return None

    task = loop.create_task(_noop())
    loop.run_until_complete(task)

    prepare_playwright_sync_thread(label="running-loop-test")
    fresh = asyncio.get_event_loop()
    assert fresh is not loop
    assert not fresh.is_closed()


def test_sync_close_same_thread_skips_executor() -> None:
    ctx = MagicMock()
    ctx.pages = []
    with patch(
        "src.automation.browser_factory._sync_close_persistent_context_impl",
    ) as impl:
        sync_close_persistent_context(ctx, log_label="t", timeout_sec=99.0, same_thread=True)
    impl.assert_called_once()
