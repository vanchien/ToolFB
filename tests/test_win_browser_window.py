"""Tests đặt/đóng cửa sổ Firefox trên Windows."""

from __future__ import annotations

from unittest.mock import patch


def test_terminate_firefox_for_profile_skips_non_windows() -> None:
    from src.utils import win_browser_window

    with patch.object(win_browser_window, "sys") as mock_sys:
        mock_sys.platform = "linux"
        assert win_browser_window.terminate_firefox_for_profile("C:/profiles/x") == 0


def test_terminate_firefox_for_profile_kills_pids() -> None:
    from src.utils import win_browser_window

    with patch.object(win_browser_window, "sys") as mock_sys:
        mock_sys.platform = "win32"
        with patch.object(
            win_browser_window,
            "_firefox_pids_for_profile",
            return_value=[111, 222],
        ):
            with patch.object(win_browser_window.time, "sleep"):
                with patch("subprocess.run") as run:
                    n = win_browser_window.terminate_firefox_for_profile(
                        "C:/ToolFB/profiles/UID_1",
                        grace_ms=0,
                    )
    assert n == 2
    assert run.call_count == 2
