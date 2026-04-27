from __future__ import annotations

import os
import subprocess
import tkinter as tk
from datetime import datetime
from pathlib import Path
from typing import Any

from tkinter import messagebox, ttk

from src.utils.paths import project_root

_INTERNAL_TOOL_DIR = project_root() / "tools" / "Veo3Studio"
_INTERNAL_TOOL_EXE = _INTERNAL_TOOL_DIR / "Veo3Studio.exe"
_EXTERNAL_TOOL_DIR = Path(r"C:\Users\Hello\Desktop\Tool")
_EXTERNAL_TOOL_EXE = _EXTERNAL_TOOL_DIR / "Veo3Studio.exe"


def ai_video_project_gate_dialog(parent: tk.Misc) -> dict[str, Any] | None:
    """
    Cổng vào tối giản cho module AI Video sạch.
    Trả về spec để tương thích với luồng gọi hiện tại trong manager_app.
    """
    ok = messagebox.askyesno(
        "AI Video Gemini/Veo",
        "Module AI Video Gemini/Veo đã được làm sạch để tích hợp tool mới.\n\n"
        "Bấm Yes để mở màn hình trống (placeholder).",
        parent=parent,
    )
    if not ok:
        return None
    return {
        "action": "open_clean_module",
        "created_at": datetime.now().replace(microsecond=0).isoformat(),
    }


class AIVideoDialog:
    """
    Placeholder trống cho AI Video Gemini/Veo.
    Dùng làm nền tích hợp tool mới do người dùng cung cấp.
    """

    def __init__(self, parent: tk.Misc, *, project_spec: dict[str, Any] | None = None) -> None:
        self._parent = parent
        self._project_spec = dict(project_spec or {})
        default_exe = _INTERNAL_TOOL_EXE if _INTERNAL_TOOL_EXE.is_file() else _EXTERNAL_TOOL_EXE
        self._tool_exe = Path(self._project_spec.get("tool_exe") or default_exe)
        self._top = tk.Toplevel(parent)
        self._top.title("AI Video Gemini/Veo — External Tool Bridge")
        self._top.geometry("900x560")
        self._top.minsize(820, 500)
        self._build_ui()

    def _build_ui(self) -> None:
        root = ttk.Frame(self._top, padding=14)
        root.pack(fill=tk.BOTH, expand=True)
        root.columnconfigure(0, weight=1)
        root.rowconfigure(2, weight=1)

        ttk.Label(
            root,
            text="AI Video Gemini/Veo (Tích hợp Tool ngoài)",
            font=("Segoe UI", 14, "bold"),
        ).grid(row=0, column=0, sticky="w")

        ttk.Label(
            root,
            text=(
                "Đã nối module AI Video Gemini/Veo với tool ngoài Veo3Studio.\n"
                "Bạn có thể mở tool trực tiếp từ đây để vận hành quy trình mới."
            ),
            justify=tk.LEFT,
            wraplength=820,
        ).grid(row=1, column=0, sticky="w", pady=(8, 8))

        launcher = ttk.LabelFrame(root, text="Bridge Launcher", padding=10)
        launcher.grid(row=2, column=0, sticky="ew")
        launcher.columnconfigure(1, weight=1)
        ttk.Label(launcher, text="Tool exe").grid(row=0, column=0, sticky="w")
        self._var_tool_exe = tk.StringVar(value=str(self._tool_exe))
        ent = ttk.Entry(launcher, textvariable=self._var_tool_exe)
        ent.grid(row=0, column=1, sticky="ew", padx=(8, 0))

        acts = ttk.Frame(launcher)
        acts.grid(row=1, column=0, columnspan=2, sticky="w", pady=(10, 0))
        ttk.Button(acts, text="Mở Veo3Studio.exe", command=self._on_launch_tool).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(acts, text="Mở thư mục Tool", command=self._on_open_tool_folder).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(acts, text="Kiểm tra đường dẫn", command=self._on_validate_tool_path).pack(side=tk.LEFT)

        box = ttk.LabelFrame(root, text="Thông tin phiên tích hợp", padding=10)
        box.grid(row=3, column=0, sticky="nsew", pady=(10, 0))
        box.columnconfigure(0, weight=1)

        spec_txt = "\n".join(
            [
                f"- action: {self._project_spec.get('action', 'open_clean_module')}",
                f"- created_at: {self._project_spec.get('created_at', '-')}",
                f"- tool_exe: {self._var_tool_exe.get()}",
                "- trạng thái: ready_for_external_tool_launch",
            ]
        )
        txt = tk.Text(box, wrap="word", height=12)
        txt.grid(row=0, column=0, sticky="nsew")
        txt.insert("1.0", spec_txt)
        txt.configure(state="disabled")

        btns = ttk.Frame(root)
        btns.grid(row=4, column=0, sticky="e", pady=(10, 0))
        ttk.Button(btns, text="Đóng", command=self._top.destroy).pack(side=tk.RIGHT)

    def _on_validate_tool_path(self) -> None:
        exe = Path(self._var_tool_exe.get().strip())
        if exe.is_file():
            messagebox.showinfo("AI Video", f"Đường dẫn hợp lệ:\n{exe}", parent=self._top)
            return
        messagebox.showwarning("AI Video", f"Không tìm thấy exe:\n{exe}", parent=self._top)

    def _on_open_tool_folder(self) -> None:
        exe = Path(self._var_tool_exe.get().strip())
        folder = exe.parent if exe.parent.exists() else _EXTERNAL_TOOL_DIR
        try:
            os.startfile(str(folder))  # type: ignore[attr-defined]
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("AI Video", f"Không mở được thư mục tool:\n{exc}", parent=self._top)

    def _on_launch_tool(self) -> None:
        exe = Path(self._var_tool_exe.get().strip())
        if not exe.is_file():
            messagebox.showwarning("AI Video", f"Không tìm thấy Veo3Studio.exe:\n{exe}", parent=self._top)
            return
        try:
            subprocess.Popen([str(exe)], cwd=str(exe.parent))
            messagebox.showinfo("AI Video", f"Đã mở tool:\n{exe}", parent=self._top)
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("AI Video", f"Mở tool thất bại:\n{exc}", parent=self._top)
