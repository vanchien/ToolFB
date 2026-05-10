from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Any, Callable


def install_treeview_shortcuts(
    tree: ttk.Treeview,
    *,
    owner: tk.Misc,
    enable_context_menu: bool = True,
    info_callback: Callable[[str], None] | None = None,
) -> None:
    """
    Gắn thao tác dùng chung cho Treeview:
    - Ctrl+A: chọn toàn bộ dòng
    - Chuột phải: Chọn hết / Bỏ chọn / Copy dòng / Copy link
    """

    if bool(getattr(tree, "_has_common_shortcuts", False)):
        return
    setattr(tree, "_has_common_shortcuts", True)

    def _notify(msg: str) -> None:
        if info_callback is not None:
            try:
                info_callback(msg)
            except Exception:
                pass

    def _select_all(_event: Any = None) -> str:
        kids = tree.get_children("")
        if kids:
            tree.selection_set(kids)
            tree.focus(kids[0])
        return "break"

    def _clear_selection() -> None:
        cur = tree.selection()
        if cur:
            tree.selection_remove(*cur)

    def _copy_rows() -> None:
        sel = list(tree.selection())
        if not sel:
            return
        lines: list[str] = []
        for iid in sel:
            vals = [str(v) for v in (tree.item(iid, "values") or ())]
            if vals:
                lines.append("\t".join(vals))
        if not lines:
            return
        owner.clipboard_clear()
        owner.clipboard_append("\n".join(lines))
        _notify(f"Đã copy {len(lines)} dòng.")

    def _copy_links() -> None:
        sel = list(tree.selection())
        if not sel:
            return
        links: list[str] = []
        for iid in sel:
            vals = [str(v) for v in (tree.item(iid, "values") or ())]
            for v in vals:
                s = v.strip()
                if not s:
                    continue
                low = s.lower()
                if "http://" in low or "https://" in low:
                    links.append(s)
        if not links:
            _notify("Không thấy link trong dòng đã chọn.")
            return
        uniq = list(dict.fromkeys(links))
        owner.clipboard_clear()
        owner.clipboard_append("\n".join(uniq))
        _notify(f"Đã copy {len(uniq)} link.")

    tree.bind("<Control-a>", _select_all, add="+")
    tree.bind("<Control-A>", _select_all, add="+")

    if not enable_context_menu:
        return

    def _on_context_menu(event: Any) -> None:
        row = tree.identify_row(event.y)
        if row:
            cur_sel = set(tree.selection())
            if row not in cur_sel:
                tree.selection_set((row,))
                tree.focus(row)
        menu = tk.Menu(owner, tearoff=0)
        menu.add_command(label="Chọn hết (Ctrl+A)", command=_select_all)
        menu.add_command(label="Bỏ chọn", command=_clear_selection)
        menu.add_separator()
        menu.add_command(label="Copy dòng đã chọn", command=_copy_rows)
        menu.add_command(label="Copy link trong dòng đã chọn", command=_copy_links)
        try:
            menu.tk_popup(event.x_root, event.y_root)
        finally:
            menu.grab_release()

    tree.bind("<Button-3>", _on_context_menu, add="+")
