"""
Dashboard web Streamlit — quản lý tài khoản, Page/Group, nội dung AI, điều phối.

Chạy từ thư mục gốc dự án::

    pip install -r requirements.txt
    streamlit run dashboard.py

Hoặc: ``python -m streamlit run dashboard.py``
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.dashboard_app.app import run_dashboard

run_dashboard()
