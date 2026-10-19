"""Hộp thoại: Page Facebook gắn nhiều TK → chọn tài khoản nào chạy."""

from __future__ import annotations

import tkinter as tk
from tkinter import messagebox, ttk
from typing import Mapping

from src.utils.page_cross_account import (
    DuplicatePageGroup,
    facebook_page_group_key,
    filter_rows_one_account_per_facebook_page,
    find_cross_account_page_groups,
    load_page_owner_choices,
    pick_row_for_group,
    preferred_account_id,
    save_page_owner_choices,
    unresolved_owner_groups,
)


def ask_page_owner_choices(
    parent: tk.Misc,
    groups: list[DuplicatePageGroup],
    *,
    current: Mapping[str, str] | None = None,
    account_labels: Mapping[str, str] | None = None,
    title: str = "Chọn tài khoản nắm Page trùng",
) -> dict[str, str] | None:
    """
    Hiện từng Page trùng với combobox tài khoản.

    Trả ``{page_key: account_id}`` hoặc ``None`` nếu hủy.
    """
    if not groups:
        return {}
    labels = dict(account_labels or {})
    saved = dict(current or {})

    top = tk.Toplevel(parent)
    top.title(title)
    top.transient(parent)
    top.geometry("640x480")
    top.minsize(520, 360)
    result: dict[str, str] | None = None

    frm = ttk.Frame(top, padding=10)
    frm.pack(fill=tk.BOTH, expand=True)
    frm.columnconfigure(0, weight=1)
    frm.rowconfigure(1, weight=1)

    ttk.Label(
        frm,
        text=(
            f"{len(groups)} Page Facebook đang gắn trên nhiều tài khoản. "
            "Chọn đúng một tài khoản để chạy (chia lịch / chia sẻ / đăng) — "
            "các TK còn lại không đăng Page đó trong lượt này, tránh lặp bài trong ngày."
        ),
        wraplength=600,
        justify="left",
    ).grid(row=0, column=0, sticky="ew", pady=(0, 8))

    box = ttk.Frame(frm)
    box.grid(row=1, column=0, sticky="nsew")
    box.columnconfigure(0, weight=1)
    box.rowconfigure(0, weight=1)
    canvas = tk.Canvas(box, highlightthickness=0)
    sy = ttk.Scrollbar(box, orient="vertical", command=canvas.yview)
    inner = ttk.Frame(canvas)
    inner.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
    canvas.create_window((0, 0), window=inner, anchor="nw")
    canvas.configure(yscrollcommand=sy.set)
    canvas.grid(row=0, column=0, sticky="nsew")
    sy.grid(row=0, column=1, sticky="ns")
    inner.columnconfigure(1, weight=1)

    ttk.Label(inner, text="Page", font=("Segoe UI", 9, "bold")).grid(row=0, column=0, sticky="w", padx=(0, 8))
    ttk.Label(inner, text="Tài khoản chạy", font=("Segoe UI", 9, "bold")).grid(row=0, column=1, sticky="w")

    vars_by_key: dict[str, tk.StringVar] = {}
    id_by_label: dict[str, dict[str, str]] = {}

    for i, group in enumerate(groups, start=1):
        acc_ids = group.account_ids
        label_list: list[str] = []
        map_lbl: dict[str, str] = {}
        for aid in acc_ids:
            lb = str(labels.get(aid) or "").strip() or aid
            if lb in map_lbl and map_lbl[lb] != aid:
                lb = f"{lb}  [{aid}]"
            map_lbl[lb] = aid
            label_list.append(lb)
        id_by_label[group.key] = map_lbl
        pick_id = preferred_account_id(group, saved)
        pick_lbl = next((lb for lb, aid in map_lbl.items() if aid == pick_id), label_list[0] if label_list else "")
        var = tk.StringVar(value=pick_lbl)
        vars_by_key[group.key] = var
        n_acc = len(acc_ids)
        ttk.Label(
            inner,
            text=f"{group.title}  ({n_acc} TK)",
            wraplength=280,
            justify="left",
        ).grid(row=i, column=0, sticky="nw", pady=4, padx=(0, 8))
        cb = ttk.Combobox(inner, textvariable=var, values=label_list, state="readonly", width=36)
        cb.grid(row=i, column=1, sticky="ew", pady=4)

    def _ok() -> None:
        nonlocal result
        picked: dict[str, str] = {}
        for group in groups:
            lbl = str(vars_by_key[group.key].get() or "").strip()
            aid = id_by_label.get(group.key, {}).get(lbl, "")
            if not aid:
                aid = preferred_account_id(group, saved)
            if aid:
                picked[group.key] = aid
        result = picked
        top.destroy()

    def _cancel() -> None:
        nonlocal result
        result = None
        top.destroy()

    btns = ttk.Frame(frm)
    btns.grid(row=2, column=0, sticky="e", pady=(10, 0))
    ttk.Button(btns, text="Hủy", command=_cancel).pack(side=tk.LEFT, padx=(0, 8))
    ttk.Button(btns, text="Dùng tài khoản đã chọn", command=_ok).pack(side=tk.LEFT)

    top.protocol("WM_DELETE_WINDOW", _cancel)
    try:
        top.grab_set()
        top.wait_window()
    except tk.TclError:
        pass
    return result


def ensure_one_account_per_facebook_page(
    parent: tk.Misc,
    rows: list[dict],
    *,
    account_labels: Mapping[str, str] | None = None,
    force_ask: bool = False,
) -> list[dict] | None:
    """
    Hỏi TK chạy cho Page trùng (nếu chưa nhớ), rồi lọc còn 1 dòng / Page.

    ``None`` = người dùng hủy. Danh sách rỗng không trùng thì trả nguyên.
    """
    groups = find_cross_account_page_groups(rows)
    if not groups:
        return list(rows)
    saved = load_page_owner_choices()
    need = groups if force_ask else unresolved_owner_groups(groups, saved)
    if need:
        labels = dict(account_labels or {})
        if not labels:
            for row in rows:
                aid = str(row.get("account_id") or "").strip()
                if not aid:
                    continue
                labels.setdefault(
                    aid,
                    str(row.get("account_label") or row.get("account_name") or aid),
                )
        picked = ask_page_owner_choices(
            parent,
            need,
            current=saved,
            account_labels=labels,
        )
        if picked is None:
            return None
        saved = save_page_owner_choices(picked)
    return filter_rows_one_account_per_facebook_page(rows, saved)


def resolve_owner_account_and_page(
    parent: tk.Misc,
    account_id: str,
    page_row: dict | None,
    all_pages: list[dict],
    *,
    account_labels: Mapping[str, str] | None = None,
) -> tuple[str, str] | None:
    """
    Page trùng nhiều TK → hỏi / dùng lựa chọn đã lưu, trả ``(account_id, page_id)`` của TK nắm Page.

    ``None`` = người dùng hủy. Không trùng thì giữ nguyên cặp đã chọn.
    """
    aid = str(account_id or "").strip()
    row = dict(page_row or {})
    local_pid = str(row.get("id") or row.get("page_id") or "").strip()
    if not aid or not local_pid:
        return (aid, local_pid)
    groups = find_cross_account_page_groups(all_pages)
    key = facebook_page_group_key(row)
    group = next((g for g in groups if g.key == key), None) if key else None
    if group is None:
        return (aid, local_pid)
    saved = load_page_owner_choices()
    need = unresolved_owner_groups([group], saved)
    if need:
        labels = dict(account_labels or {})
        if not labels:
            for item in group.rows:
                other = str(item.get("account_id") or "").strip()
                if other:
                    labels.setdefault(
                        other,
                        str(item.get("account_label") or item.get("account_name") or other),
                    )
        picked = ask_page_owner_choices(parent, need, current=saved, account_labels=labels)
        if picked is None:
            return None
        saved = save_page_owner_choices(picked)
    owner = preferred_account_id(group, saved)
    picked_row = pick_row_for_group(group, owner) or row
    owner_pid = str(picked_row.get("id") or picked_row.get("page_id") or local_pid).strip()
    if owner and owner != aid:
        messagebox.showinfo(
            "Page trùng nhiều tài khoản",
            (
                f"«{group.title}» đang gắn {len(group.account_ids)} tài khoản. "
                "Job sẽ chạy trên tài khoản đã chọn nắm Page — các TK còn lại không đăng Page này."
            ),
            parent=parent,
        )
    return (owner or aid, owner_pid or local_pid)
