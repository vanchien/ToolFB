"""Thư mục / filter mặc định cho hộp thoại chọn file media (job lịch, Video Editor)."""

from __future__ import annotations

from pathlib import Path
from tkinter import filedialog

from src.services.video_editor.layout import ensure_video_editor_layout

# Windows cần dấu ; giữa pattern — "*.mp4 *.mkv" khiến hộp thoại lọc sai / chỉ «import» một kiểu file.
_VIDEO_GLOB_WIN = "*.mp4;*.mkv;*.mov;*.webm;*.avi;*.m4v"

VIDEO_MEDIA_FILETYPES: tuple[tuple[str, str], ...] = (
    ("Video (mp4, mkv, mov…)", _VIDEO_GLOB_WIN),
    ("Tất cả", "*.*"),
)


def default_video_media_initialdir(*, path_hint: str = "") -> str:
    """
    Thư mục mở sẵn khi chọn video — ưu tiên thư mục file đang nhập, không thì ``data/video_editor/renders``.
    """
    hint = str(path_hint or "").strip()
    if hint:
        p = Path(hint).expanduser()
        try:
            if p.is_file():
                return str(p.parent.resolve())
            if p.is_dir():
                return str(p.resolve())
        except OSError:
            pass
    renders = ensure_video_editor_layout()["renders"]
    renders.mkdir(parents=True, exist_ok=True)
    return str(renders.resolve())


def pick_video_media_files(
    parent,
    *,
    path_hint: str = "",
    title: str = "Chọn file video",
    multiple: bool = True,
) -> tuple[str, ...]:
    """
    Hộp thoại chọn video (không copy vào thư viện VE) — mở đúng thư mục renders / thư mục file hiện có.

    Tránh tiêu đề mặc định Windows «File Upload» và filter ``*.mp4 *.mkv`` bị lỗi trên Win32.
    """
    initialdir = default_video_media_initialdir(path_hint=path_hint)
    kw: dict = {
        "parent": parent,
        "title": title,
        "filetypes": list(VIDEO_MEDIA_FILETYPES),
        "initialdir": initialdir,
    }
    if multiple:
        raw = filedialog.askopenfilenames(**kw)
        return tuple(str(p) for p in raw) if raw else ()
    one = filedialog.askopenfilename(**kw)
    return (str(one),) if one else ()
