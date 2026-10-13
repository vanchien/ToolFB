"""Giao diện chung cho mọi tab: chữ, nút, bảng và khung cùng một cỡ."""

from __future__ import annotations

import tkinter as tk
from tkinter import ttk

FONT_UI = ("Segoe UI", 9)
FONT_UI_BOLD = ("Segoe UI", 9, "bold")


def apply_app_theme(root: tk.Misc) -> None:
    """Áp style ttk một lần cho cửa sổ chính và mọi hộp thoại con."""
    style = ttk.Style(root)
    try:
        style.configure("TLabel", font=FONT_UI)
        style.configure("TButton", font=FONT_UI, padding=(8, 3))
        style.configure("TCheckbutton", font=FONT_UI)
        style.configure("TRadiobutton", font=FONT_UI)
        style.configure("TLabelframe.Label", font=FONT_UI_BOLD, foreground="#334155")
        style.configure("TNotebook.Tab", padding=(12, 6), font=FONT_UI)
        style.configure("Treeview", rowheight=24, font=FONT_UI)
        style.configure("Treeview.Heading", font=FONT_UI_BOLD)
    except tk.TclError:
        return
