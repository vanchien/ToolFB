"""Delay ngẫu nhiên cho automation (gõ phím, chờ UI)."""

from __future__ import annotations

import random


def random_ms(low: int, high: int) -> int:
    """Trả về số mili giây nguyên trong ``[low, high]`` (hai đầu gồm)."""
    if high < low:
        low, high = high, low
    return int(random.randint(low, high))
