"""Tests HumanAction Bezier và profile tỷ lệ Like/Comment."""

from __future__ import annotations

import math

from src.services.human_interaction_profile import resolve_profile
from src.utils.human_action import _bezier_points, _cubic_bezier


def test_bezier_endpoints() -> None:
    start = (100.0, 200.0)
    end = (400.0, 500.0)
    pts = _bezier_points(start, end, steps=20)
    assert len(pts) == 21
    assert math.isclose(pts[0][0], start[0], abs_tol=1.0)
    assert math.isclose(pts[0][1], start[1], abs_tol=1.0)
    assert math.isclose(pts[-1][0], end[0], abs_tol=1.0)
    assert math.isclose(pts[-1][1], end[1], abs_tol=1.0)


def test_cubic_bezier_midpoint_not_linear_only() -> None:
    p0 = (0.0, 0.0)
    p3 = (100.0, 0.0)
    mid = _cubic_bezier(p0, (20.0, 80.0), (80.0, -60.0), p3, 0.5)
    assert abs(mid[1]) > 1.0


def test_resolve_profile_behavior_overrides() -> None:
    p = resolve_profile(
        "normal",
        settings={
            "like_rate_pct": 30,
            "comment_rate_pct": 10,
            "virtual_cursor": False,
            "ai_comments": True,
        },
    )
    assert abs(p.like_rate_pct - 0.30) < 0.001
    assert abs(p.comment_rate_pct - 0.10) < 0.001
    assert p.virtual_cursor is False
    assert p.ai_comments is True


def test_resolve_profile_fraction_rates() -> None:
    p = resolve_profile("safe", settings={"like_rate_pct": 0.25})
    assert abs(p.like_rate_pct - 0.25) < 0.001
