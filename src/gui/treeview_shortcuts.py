from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Any, Callable


def _widget_is_tree_or_child(widget: tk.Misc | None, tree: ttk.Treeview) -> bool:
    w: tk.Misc | None = widget
    while w is not None:
        if w == tree:
            return True
        try:
            w = w.master  # type: ignore[assignment]
        except (AttributeError, tk.TclError):
            break
    return False


def install_treeview_shortcuts(
    tree: ttk.Treeview,
    *,
    owner: tk.Misc,
    enable_context_menu: bool = True,
    enable_drag_select: bool = True,
    info_callback: Callable[[str], None] | None = None,
) -> None:
    """
    Gắn thao tác dùng chung cho Treeview:
    - Ctrl+A: chọn toàn bộ dòng (khi focus trong bảng)
    - Kéo chuột: chọn dải dòng (giữ nút trái, kéo qua nhiều dòng)
    - Shift+click: chọn dải (mặc định ``selectmode=extended``)
    - Chuột phải: Chọn hết / Bỏ chọn / Copy dòng / Copy link
    """

    if bool(getattr(tree, "_has_common_shortcuts", False)):
        return
    setattr(tree, "_has_common_shortcuts", True)

    drag_state: dict[str, str | None] = {"anchor": None}

    def _notify(msg: str) -> None:
        if info_callback is not None:
            try:
                info_callback(msg)
            except Exception:
                pass

    def _all_row_ids() -> tuple[str, ...]:
        return tuple(tree.get_children(""))

    def _select_all(_event: Any = None) -> str:
        kids = _all_row_ids()
        if kids:
            tree.selection_set(kids)
            tree.focus(kids[0])
            tree.see(kids[0])
            _notify(f"Đã chọn {len(kids)} dòng.")
        return "break"

    def _select_range(anchor: str, end_row: str) -> None:
        kids = list(_all_row_ids())
        if not kids or anchor not in kids or end_row not in kids:
            return
        ia, ib = kids.index(anchor), kids.index(end_row)
        lo, hi = min(ia, ib), max(ia, ib)
        tree.selection_set(kids[lo : hi + 1])
        tree.focus(end_row)
        tree.see(end_row)

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

    def _on_ctrl_a(event: Any) -> str | None:
        if _widget_is_tree_or_child(owner.focus_get(), tree):
            return _select_all(event)
        return None

    tree.bind("<Control-a>", _on_ctrl_a, add="+")
    tree.bind("<Control-A>", _on_ctrl_a, add="+")

    registry: set[ttk.Treeview] = getattr(owner, "_toolfb_treeview_registry", set())
    registry.add(tree)
    setattr(owner, "_toolfb_treeview_registry", registry)

    if not getattr(owner, "_toolfb_treeview_ctrl_a_bound", False):

        def _global_ctrl_a(event: Any) -> str | None:
            w = owner.focus_get()
            for tv in getattr(owner, "_toolfb_treeview_registry", ()):
                if _widget_is_tree_or_child(w, tv):
                    kids = tuple(tv.get_children(""))
                    if kids:
                        tv.selection_set(kids)
                        tv.focus(kids[0])
                        tv.see(kids[0])
                    return "break"
            return None

        owner.bind_all("<Control-a>", _global_ctrl_a, add="+")
        owner.bind_all("<Control-A>", _global_ctrl_a, add="+")
        setattr(owner, "_toolfb_treeview_ctrl_a_bound", True)

    def _on_tree_click_focus(event: Any) -> None:
        try:
            tree.focus_set()
        except tk.TclError:
            pass

    tree.bind("<Button-1>", _on_tree_click_focus, add="+")

    if enable_drag_select:

        def _on_press_drag(event: Any) -> None:
            region = tree.identify_region(event.x, event.y)
            if region in ("heading", "separator", "nothing"):
                drag_state["anchor"] = None
                return
            row = tree.identify_row(event.y)
            if not row:
                drag_state["anchor"] = None
                return
            drag_state["anchor"] = row
            # Click thường (không Shift/Ctrl): neo chọn tại dòng bấm — kéo sẽ mở rộng dải.
            state = int(getattr(event, "state", 0) or 0)
            if not (state & 0x1) and not (state & 0x4):  # không Shift, không Control
                tree.selection_set((row,))
                tree.focus(row)

        def _on_motion_drag(event: Any) -> None:
            if not (int(getattr(event, "state", 0) or 0) & 0x100):
                return
            anchor = drag_state.get("anchor")
            if not anchor:
                return
            row = tree.identify_row(event.y)
            if not row:
                return
            _select_range(str(anchor), row)

        def _on_release_drag(_event: Any) -> None:
            drag_state["anchor"] = None

        tree.bind("<ButtonPress-1>", _on_press_drag, add="+")
        tree.bind("<B1-Motion>", _on_motion_drag, add="+")
        tree.bind("<ButtonRelease-1>", _on_release_drag, add="+")

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
