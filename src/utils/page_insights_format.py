"""Định dạng số followers/views cho UI."""

from __future__ import annotations

import re


def parse_metric_number(raw: str | int | float | None) -> int | None:
    """
    Chuyển chuỗi Meta (``12,345``, ``1.2K``, ``3M``) → int.
    """
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        try:
            v = int(raw)
            return v if v >= 0 else None
        except (TypeError, ValueError):
            return None
    s = str(raw).strip().replace("\u00a0", " ")
    if not s or s in {"—", "-", "N/A", "n/a"}:
        return None
    s = re.sub(r"\s+", "", s)
    m = re.match(r"^([\d.,]+)\s*([KMB])?$", s, flags=re.I)
    if not m:
        digits = re.sub(r"[^\d]", "", s)
        return int(digits) if digits else None
    num_part, suffix = m.group(1), (m.group(2) or "").upper()
    if "," in num_part and "." in num_part:
        if num_part.rfind(",") > num_part.rfind("."):
            num_part = num_part.replace(".", "").replace(",", ".")
        else:
            num_part = num_part.replace(",", "")
    elif "," in num_part:
        parts = num_part.split(",")
        if len(parts) > 1 and all(len(p) == 3 for p in parts[1:]):
            num_part = "".join(parts)
        else:
            num_part = num_part.replace(",", ".")
    try:
        base = float(num_part)
    except ValueError:
        return None
    mult = {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000}.get(suffix, 1)
    return int(base * mult)


def format_metric(n: int | None) -> str:
    if n is None:
        return "—"
    if n >= 1_000_000_000:
        return f"{n / 1_000_000_000:.1f}B".replace(".0B", "B")
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M".replace(".0M", "M")
    if n >= 10_000:
        return f"{n / 1_000:.1f}K".replace(".0K", "K")
    return f"{n:,}".replace(",", ".")
