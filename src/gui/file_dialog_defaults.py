"""Thư mục / filter mặc định cho hộp thoại chọn file media (job lịch, Video Editor).

Quan trọng: chỉ trả về **đường dẫn gốc** trên máy người dùng — **không copy / không import**
vào thư viện app. Đăng bài dùng path này trực tiếp khi upload Facebook.
"""

from __future__ import annotations

import json
from pathlib import Path
from tkinter import filedialog

from src.services.video_editor.layout import ensure_video_editor_layout
from src.utils.paths import project_root

# Windows cần dấu ; giữa pattern — "*.mp4 *.mkv" khiến hộp thoại lọc sai / chỉ «import» một kiểu file.
_VIDEO_GLOB_WIN = "*.mp4;*.mkv;*.mov;*.webm;*.avi;*.m4v"

VIDEO_MEDIA_FILETYPES: tuple[tuple[str, str], ...] = (
    ("Video (mp4, mkv, mov…)", _VIDEO_GLOB_WIN),
    ("Tất cả", "*.*"),
)

_LAST_DIR_NAME = "video_pick_last_dir.json"


def _last_dir_store() -> Path:
    d = project_root() / "data" / "runtime"
    d.mkdir(parents=True, exist_ok=True)
    return d / _LAST_DIR_NAME


def _load_last_video_dir() -> str:
    path = _last_dir_store()
    if not path.is_file():
        return ""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        raw = str((data or {}).get("path") or "").strip()
        if raw and Path(raw).is_dir():
            return str(Path(raw).resolve())
    except Exception:
        return ""
    return ""


def _save_last_video_dir(folder: str) -> None:
    folder = str(folder or "").strip()
    if not folder:
        return
    try:
        p = Path(folder).expanduser().resolve()
        if not p.is_dir():
            return
        _last_dir_store().write_text(
            json.dumps({"path": str(p)}, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    except Exception:
        return


def default_video_media_initialdir(*, path_hint: str = "") -> str:
    """
    Thư mục mở sẵn khi chọn video:

    1. Thư mục của file/path đang nhập
    2. Lần chọn trước (không copy file — chỉ nhớ thư mục)
    3. ``data/video_editor/renders``
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
    last = _load_last_video_dir()
    if last:
        return last
    renders = ensure_video_editor_layout()["renders"]
    renders.mkdir(parents=True, exist_ok=True)
    return str(renders.resolve())


def pick_video_media_files(
    parent,
    *,
    path_hint: str = "",
    title: str = "Chọn video từ thư mục (chỉ đường dẫn — không copy vào ToolFB)",
    multiple: bool = True,
) -> tuple[str, ...]:
    """
    Hộp thoại chọn video — **chỉ trả path gốc**, không import/copy vào thư viện VE hay drafts.

    Tránh tiêu đề mặc định Windows «File Upload» và filter ``*.mp4 *.mkv`` bị lỗi trên Win32.
    Job lịch đăng / đăng bài dùng path này trực tiếp với Playwright ``set_input_files``.
    """
    initialdir = default_video_media_initialdir(path_hint=path_hint)
    # Tiêu đề rõ ràng — Windows đôi khi vẫn hiện «File Upload» nhưng path vẫn là file gốc.
    safe_title = str(title or "").strip() or (
        "Chọn video từ thư mục (chỉ đường dẫn — không copy vào ToolFB)"
    )
    kw: dict = {
        "parent": parent,
        "title": safe_title,
        "filetypes": list(VIDEO_MEDIA_FILETYPES),
        "initialdir": initialdir,
    }
    if multiple:
        raw = filedialog.askopenfilenames(**kw)
        paths = tuple(str(p) for p in raw) if raw else ()
    else:
        one = filedialog.askopenfilename(**kw)
        paths = (str(one),) if one else ()
    if paths:
        try:
            _save_last_video_dir(str(Path(paths[0]).expanduser().resolve().parent))
        except OSError:
            pass
    return paths
