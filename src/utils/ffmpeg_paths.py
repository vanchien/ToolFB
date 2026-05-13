"""
Đường dẫn ffmpeg/ffprobe/ffplay dùng chung (PATH hoặc tools/ffmpeg/bin portable).
Tránh trùng logic giữa GUI lịch đăng, AI Video thumbnail, Video Editor, v.v.
"""

from __future__ import annotations

import os
import shutil
import urllib.request
import zipfile
from pathlib import Path

from loguru import logger

from src.utils.paths import project_root

# Chỉ tự tải zip ffplay một lần mỗi process (sao chép từ máy có thể lặp lại an toàn).
_ffplay_auto_fetch_attempted: bool = False


def portable_ffmpeg_bin_dir() -> Path:
    return project_root() / "tools" / "ffmpeg" / "bin"


def _ffplay_exe_name() -> str:
    return "ffplay.exe" if os.name == "nt" else "ffplay"


def _ffplay_portable_path() -> Path:
    return portable_ffmpeg_bin_dir() / _ffplay_exe_name()


def _iter_ffplay_source_candidates() -> list[Path]:
    """
    Các vị trí có thể có ffplay để **copy** vào portable.

    Ưu tiên ffplay **cùng thư mục với ffmpeg/ffprobe** mà app đang dùng (thường bản mới, có AV1),
    rồi mới tới ``shutil.which("ffplay")`` (tránh bản PATH rất cũ như 2.7.x).
    """
    exe = _ffplay_exe_name()
    out: list[Path] = []
    seen: set[str] = set()

    def push(p: Path) -> None:
        try:
            r = p.resolve()
        except OSError:
            r = p
        k = str(r).lower()
        if k in seen:
            return
        seen.add(k)
        out.append(r)

    ff, ffp = resolve_ffmpeg_ffprobe_paths()
    for base in (ff, ffp):
        if not base:
            continue
        try:
            sib = Path(base).resolve().parent / exe
            if sib.is_file():
                push(sib)
        except OSError:
            continue
    w = shutil.which("ffplay")
    if w:
        push(Path(w))
    if os.name == "nt":
        for key in ("ProgramFiles", "ProgramFiles(x86)"):
            root = os.environ.get(key, "").strip()
            if root:
                p = Path(root) / "ffmpeg" / "bin" / exe
                if p.is_file():
                    push(p)
    return out


def _try_copy_ffplay_into_portable() -> bool:
    """
    Sao chép ffplay đã có trên máy vào ``tools/ffmpeg/bin`` (trùng đích thì bỏ qua).

    Returns:
        ``True`` nếu sau khi copy (hoặc đã có sẵn) file portable tồn tại.
    """
    local = _ffplay_portable_path()
    if local.is_file():
        return True
    try:
        bin_dir = portable_ffmpeg_bin_dir()
        bin_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        logger.warning("Không tạo được thư mục {}: {}", portable_ffmpeg_bin_dir(), exc)
        return False

    for src in _iter_ffplay_source_candidates():
        if not src.is_file():
            continue
        try:
            if src.resolve() == local.resolve():
                return True
            shutil.copy2(src, local)
            logger.info("Đã copy ffplay vào {} từ {}", local, src)
            return True
        except OSError as exc:
            logger.debug("Không copy ffplay từ {}: {}", src, exc)
    return local.is_file()


def resolve_ffmpeg_ffprobe_paths() -> tuple[str | None, str | None]:
    """Ưu tiên ffmpeg+ffprobe trên PATH; nếu thiếu một trong hai thì thử bản portable trong repo."""
    ffmpeg = shutil.which("ffmpeg")
    ffprobe = shutil.which("ffprobe")
    if ffmpeg and ffprobe:
        return ffmpeg, ffprobe
    exe_ffmpeg = "ffmpeg.exe" if os.name == "nt" else "ffmpeg"
    exe_ffprobe = "ffprobe.exe" if os.name == "nt" else "ffprobe"
    pff = portable_ffmpeg_bin_dir() / exe_ffmpeg
    pfp = portable_ffmpeg_bin_dir() / exe_ffprobe
    if pff.is_file() and pfp.is_file():
        return str(pff), str(pfp)
    return ffmpeg, ffprobe


def resolve_ffmpeg_executable() -> str | None:
    """Chỉ cần ffmpeg (thumbnail, transcode đơn giản)."""
    ff, _ = resolve_ffmpeg_ffprobe_paths()
    return ff


def ensure_ffplay_portable() -> bool:
    """
    Đảm bảo có ffplay trong ``tools/ffmpeg/bin``:

    1. Đã có sẵn trong bin → xong.
    2. Tìm trên máy (cạnh ffmpeg/ffprobe → PATH → Program Files\\ffmpeg\\bin) → **copy** vào bin.
    3. (Windows) Vẫn thiếu → tải zip **full** Gyan, chỉ trích ffplay.

    Tắt tải mạng (vẫn copy nếu tìm thấy): ``TOOLFB_NO_AUTO_FFPLAY=1``.

    Returns:
        ``True`` nếu sau khi gọi đã dùng được ffplay (portable hoặc PATH).
    """
    global _ffplay_auto_fetch_attempted
    if os.environ.get("TOOLFB_NO_AUTO_FFPLAY", "").strip().lower() in {"1", "true", "yes", "on"}:
        return bool(shutil.which("ffplay")) or _ffplay_portable_path().is_file()

    local = _ffplay_portable_path()
    if local.is_file():
        return True

    if _try_copy_ffplay_into_portable():
        return True

    if shutil.which("ffplay"):
        return True

    exe = _ffplay_exe_name()
    if os.name != "nt":
        logger.debug("Tự tải ffplay zip: chỉ hỗ trợ Windows (nt).")
        return False

    if _ffplay_auto_fetch_attempted:
        return local.is_file()
    _ffplay_auto_fetch_attempted = True

    root = project_root() / "tools" / "ffmpeg"
    download_dir = root / "downloads"
    extract_dir = root / "extracted_ffplay"
    download_dir.mkdir(parents=True, exist_ok=True)
    extract_dir.mkdir(parents=True, exist_ok=True)
    bin_dir = portable_ffmpeg_bin_dir()
    bin_dir.mkdir(parents=True, exist_ok=True)

    url = os.environ.get(
        "TOOLFB_FFPLAY_FULL_ZIP_URL",
        "https://www.gyan.dev/ffmpeg/builds/ffmpeg-release-full.zip",
    ).strip()
    zip_path = download_dir / "ffmpeg-release-full-ffplay.zip"
    try:
        logger.info("Không tìm thấy ffplay để copy — đang tải zip full Gyan → {} …", bin_dir)
        urllib.request.urlretrieve(url, str(zip_path))
        with zipfile.ZipFile(zip_path, "r") as zf:
            members = zf.namelist()
            play_member = next((m for m in members if m.lower().endswith("/bin/ffplay.exe")), "")
            if play_member:
                with zf.open(play_member) as src, local.open("wb") as dst:
                    dst.write(src.read())
            else:
                zf.extractall(extract_dir)
                found = None
                for p in extract_dir.rglob("ffplay.exe"):
                    if p.is_file():
                        found = p
                        break
                if not found:
                    logger.warning("Zip full không chứa ffplay.exe — kiểm tra URL / layout Gyan.")
                    return False
                shutil.copy2(found, local)
        keep = os.environ.get("FFMPEG_KEEP_INSTALL_CACHE", "0").strip().lower() in {"1", "true", "yes", "on"}
        if not keep:
            try:
                shutil.rmtree(extract_dir, ignore_errors=True)
            except Exception:
                pass
            try:
                if zip_path.is_file():
                    zip_path.unlink()
            except Exception:
                pass
        if local.is_file():
            logger.info("Đã đặt ffplay tại: {}", local)
            return True
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không tải/giải nén được ffplay portable: {}", exc)
    return bool(local.is_file())


def ffplay_resolve_skips_ensure_heavy_work() -> bool:
    """
    ``True`` nếu ``resolve_ffplay_executable()`` có thể trả lời mà **không** gọi
    ``ensure_ffplay_portable()`` (tránh copy/tải zip đồng bộ — thường gây treo UI).

    Heuristic: ffplay portable đã có, hoặc ffplay cạnh ffmpeg/ffprobe đã phát hiện,
    hoặc ``shutil.which("ffplay")``.
    """
    exe = _ffplay_exe_name()
    p = portable_ffmpeg_bin_dir() / exe
    if p.is_file():
        return True
    ff, ffp = resolve_ffmpeg_ffprobe_paths()
    for base in (ff, ffp):
        if not base:
            continue
        try:
            sibling = Path(base).resolve().parent / exe
            if sibling.is_file():
                return True
        except OSError:
            continue
    if shutil.which("ffplay"):
        return True
    return False


def resolve_ffplay_executable() -> str | None:
    """
    ffplay: **portable** ``tools/ffmpeg/bin`` → cùng thư mục ``ffmpeg``/``ffprobe`` đã phát hiện
    → ``PATH`` → ``ensure_ffplay_portable()`` (copy từ máy hoặc tải zip trên Windows).

    Không ưu tiên PATH trước: nhiều máy có ffplay 2.7.x trên PATH (không AV1) trong khi
    ffmpeg mới nằm cạnh hoặc trong bundle — video YouTube dạng AV1 sẽ không mở được.
    """
    exe = _ffplay_exe_name()
    p = portable_ffmpeg_bin_dir() / exe
    if p.is_file():
        return str(p)
    ff, ffp = resolve_ffmpeg_ffprobe_paths()
    for base in (ff, ffp):
        if not base:
            continue
        try:
            sibling = Path(base).resolve().parent / exe
            if sibling.is_file():
                return str(sibling)
        except OSError:
            continue
    fp = shutil.which("ffplay")
    if fp:
        return fp
    if ensure_ffplay_portable() and p.is_file():
        return str(p)
    return None
