"""Gõ phím giống người — delay 50–200 ms."""

from __future__ import annotations

from src.utils.human_typing import _normalize_typed, human_typing_delay_ms


def test_human_typing_delay_in_default_range() -> None:
    for _ in range(20):
        d = human_typing_delay_ms()
        assert 50 <= d <= 200


def test_normalize_typed_for_search_verify() -> None:
    assert _normalize_typed("  Reels Hài  ") == "reels hài"
    assert _normalize_typed("Nấu Ăn") == "nấu ăn"
