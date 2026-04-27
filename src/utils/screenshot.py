"""
Chụp màn hình khi lỗi automation (stub — mở rộng sau).

Playwright: ``page.screenshot(path=...)`` nên gọi từ ``post_executor`` khi có ``page``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from loguru import logger

from src.utils.paths import project_root


def screenshots_dir() -> Path:
    d = project_root() / "logs" / "screenshots"
    d.mkdir(parents=True, exist_ok=True)
    return d


def capture_page_screenshot(page: Any, stem: str) -> Path | None:
    """
    Lưu PNG vào ``logs/screenshots/{stem}.png`` nếu ``page`` hỗ trợ ``screenshot``.

    Returns:
        Path file hoặc None nếu không chụp được.
    """
    path = screenshots_dir() / f"{stem}.png"
    try:
        fn = getattr(page, "screenshot", None)
        if callable(fn):
            fn(path=str(path))  # type: ignore[misc]
            logger.warning("Đã chụp screenshot lỗi: {}", path)
            return path
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không chụp screenshot ({}): {}", stem, exc)
    return None
