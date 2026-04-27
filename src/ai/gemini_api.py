"""
Kết nối Gemini — facade tách khỏi UI / draft store.

Logic sinh bài nằm tại ``content_creator.py``; module này chỉ re-export API ổn định.
"""

from __future__ import annotations

from src.ai.content_creator import generate_post

__all__ = ["generate_post"]
