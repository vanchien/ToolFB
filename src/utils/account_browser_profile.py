"""
Profile Playwright/Firefox riêng cho từng ``account_id`` — tạo khi thêm, xóa khi gỡ tài khoản.

Mỗi tài khoản: ``data/profiles/<browser>/<id>`` + ``data/cookies/<id>.json`` (không dùng chung thư mục).
"""

from __future__ import annotations

import json
import re
import shutil
import uuid
from pathlib import Path
from typing import Any

from loguru import logger

from src.utils.paths import project_root
from src.utils.safe_delete import safe_delete_path

_BROWSER_DIRS = frozenset({"chromium", "chrome", "firefox", "webkit"})

_EMPTY_STORAGE_STATE = json.dumps({"cookies": [], "origins": []}, ensure_ascii=False, indent=2) + "\n"

# Marker nhận diện profile Firefox / Chromium khi import từ thư mục ngoài dự án.
_FIREFOX_PROFILE_MARKERS = (
    "prefs.js",
    "cookies.sqlite",
    "places.sqlite",
    "cert9.db",
    "key4.db",
    "handlers.json",
    "compatibility.ini",
)
_CHROMIUM_PROFILE_MARKERS = (
    "Local State",
    "Preferences",
    "Default/Preferences",
    "Default/Cookies",
    "Default/Network/Cookies",
)
# Thư mục con thường gặp trong Firefox — không phải profile độc lập khi quét cha.
_PROFILE_SCAN_SKIP_NAMES = frozenset(
    {
        "cache2",
        "startupcache",
        "thumbnails",
        "safebrowsing",
        "crashes",
        "minidumps",
        "datareporting",
        "saved-telemetry-pings",
        "shader-cache",
        "gpu-cache",
        "jumplistcache",
        "storage",
        "sessionstore-backups",
        "bookmarkbackups",
        "chrome",
        "extensions",
        "features",
        "gmp-gmpopenh264",
        "gmp-widevinecdm",
        "browser-extension-data",
        "weave",
        "default",
        "system",
        "tmp",
        "temp",
        "__pycache__",
        # Firefox Portable wrapper — không phải 1 tài khoản riêng.
        "app",
        "data",
        "other",
        "firefox",
        "firefox64",
        "firefox32",
        "settings",
        "profile",
        "profiles",
    }
)

# Tên exe / thư mục đặc trưng Firefox Portable (PortableApps).
_FIREFOX_PORTABLE_EXE_NAMES = frozenset(
    {
        "firefoxportable.exe",
        "firefox portable.exe",
        "firefox_portable.exe",
    }
)


def normalize_browser_storage(browser_type: str) -> str:
    bt = str(browser_type or "firefox").strip().lower()
    if bt == "chrome":
        return "chromium"
    if bt in _BROWSER_DIRS:
        return bt
    return "firefox"


def default_portable_path(account_id: str, browser_type: str = "firefox") -> str:
    aid = str(account_id or "").strip()
    sub = normalize_browser_storage(browser_type)
    return f"data/profiles/{sub}/{aid}"


def default_cookie_path(account_id: str) -> str:
    return f"data/cookies/{str(account_id or '').strip()}.json"


def relativize_account_storage_path(
    raw: str,
    *,
    project_root_dir: Path | None = None,
) -> str:
    """
    Chuyển đường dẫn tuyệt đối (máy cũ) → relative ``data/...`` để copy cả thư mục ToolFB sang máy khác.

    Giữ nguyên nếu đã relative hoặc nằm ngoài project (vd. Chrome hệ thống).
    """
    s = str(raw or "").strip()
    if not s:
        return s
    root = (project_root_dir or project_root()).resolve()
    p = Path(s).expanduser()
    if not p.is_absolute():
        return s.replace("\\", "/")
    try:
        rel = p.resolve().relative_to(root)
        return rel.as_posix()
    except ValueError:
        return str(p.resolve())


def is_foreign_browser_exe_path(
    raw: str,
    *,
    project_root_dir: Path | None = None,
) -> bool:
    """
    True nếu ``browser_exe_path`` trỏ Firefox/Chrome Via/GPM hoặc ngoài thư mục ToolFB.

    Sau khi import, profile đã nằm ``data/profiles/…`` — exe Via không cần và hay làm treo.
    Layout ``App/Firefox`` / ``App/Chrome-bin`` / ``FirefoxPortable.exe`` luôn bỏ.
    """
    s = str(raw or "").strip()
    if not s:
        return False
    root = (project_root_dir or project_root()).resolve()
    p = Path(s).expanduser()
    parts = [part.casefold() for part in p.parts]
    name = Path(s).name.casefold()
    via_layout = (
        ("app" in parts and ("firefox" in parts or "chrome-bin" in parts))
        or name == "firefoxportable.exe"
        or ("portable" in name and "firefox" in name and name.endswith(".exe"))
    )
    if via_layout:
        return True
    try:
        resolved = p.resolve() if p.is_absolute() else (root / p).resolve()
    except OSError:
        return True
    try:
        resolved.relative_to(root)
        return False
    except ValueError:
        # Ngoài project (ổ D:, máy khác…).
        return True


def strip_foreign_browser_exe_path(
    raw: str,
    *,
    project_root_dir: Path | None = None,
) -> str:
    """Trả ``\"\"`` nếu exe ngoài ToolFB; giữ nguyên nếu rỗng hoặc nằm trong project."""
    s = str(raw or "").strip()
    if not s:
        return ""
    if is_foreign_browser_exe_path(s, project_root_dir=project_root_dir):
        return ""
    return s


def resolve_account_path(project_root_dir: Path, raw: str) -> Path | None:
    s = str(raw or "").strip()
    if not s:
        return None
    p = Path(s)
    if not p.is_absolute():
        p = project_root_dir / p
    try:
        return p.resolve()
    except OSError:
        return None


def assert_portable_path_not_shared(
    account_id: str,
    portable_path: str,
    other_accounts: list[dict[str, Any]],
    *,
    project_root_dir: Path | None = None,
) -> None:
    """Không cho hai id khác nhau trỏ cùng một thư mục profile."""
    root = (project_root_dir or project_root()).resolve()
    aid = str(account_id or "").strip()
    mine = resolve_account_path(root, portable_path)
    if mine is None:
        return
    for other in other_accounts:
        oid = str(other.get("id", "")).strip()
        if not oid or oid == aid:
            continue
        for key in ("portable_path", "profile_path"):
            op = resolve_account_path(root, str(other.get(key) or ""))
            if op is not None and op == mine:
                raise ValueError(
                    f"portable_path trùng tài khoản «{oid}» ({mine}). "
                    "Mỗi tài khoản phải có profile Firefox/Playwright riêng."
                )


def provision_fresh_browser_profile(
    account_id: str,
    browser_type: str = "firefox",
    *,
    project_root_dir: Path | None = None,
    cookie_path: str | None = None,
) -> tuple[str, str]:
    """
    Tạo profile Playwright **mới** cho một tài khoản (xóa thư mục cũ cùng đường dẫn nếu có).

    Returns:
        ``(portable_path, cookie_path)`` dạng relative tới project root.
    """
    root = (project_root_dir or project_root()).resolve()
    aid = str(account_id or "").strip()
    if not aid:
        raise ValueError("account_id rỗng — không tạo profile.")
    portable_rel = default_portable_path(aid, browser_type)
    cookie_rel = str(cookie_path or "").strip() or default_cookie_path(aid)
    profile_dir = resolve_account_path(root, portable_rel)
    cookie_file = resolve_account_path(root, cookie_rel)
    if profile_dir is None or cookie_file is None:
        raise ValueError("Đường dẫn profile/cookie không hợp lệ.")

    profiles_root = (root / "data" / "profiles").resolve()
    cookies_root = (root / "data" / "cookies").resolve()
    if profile_dir.exists():
        logger.info("Xóa profile cũ trước khi tạo mới | account={} | {}", aid, profile_dir)
        safe_delete_path(profile_dir, allowed_roots=[profiles_root], kind="dir", missing_ok=True)
    profile_dir.mkdir(parents=True, exist_ok=True)
    _write_profile_owner_marker(profile_dir, aid)
    if normalize_browser_storage(browser_type) == "firefox":
        try:
            from src.utils.firefox_portable_profile import ensure_firefox_portable_launcher

            ensure_firefox_portable_launcher(profile_dir)
        except Exception as exc:  # noqa: BLE001
            logger.debug("Không ghi Mo_Firefox.bat (profile mới): {}", exc)

    if cookie_file.exists():
        safe_delete_path(cookie_file, allowed_roots=[cookies_root], kind="file", missing_ok=True)
    cookie_file.parent.mkdir(parents=True, exist_ok=True)
    cookie_file.write_text(_EMPTY_STORAGE_STATE, encoding="utf-8")

    logger.info(
        "Đã tạo profile Playwright riêng | account={} | browser={} | profile={}",
        aid,
        normalize_browser_storage(browser_type),
        profile_dir,
    )
    return portable_rel, cookie_rel


def ensure_account_browser_profile_ready(acc: dict[str, Any]) -> None:
    """
    Chuẩn bị ``acc`` trước ``launch_persistent_context``: registry, đường dẫn profile, mkdir.

    Tránh lỗi «Thư mục profile không tồn tại» khi mở trình duyệt tay lần đầu.
    """
    aid = str(acc.get("registry_id") or acc.get("id") or "").strip() or "manual"
    portable = resolve_account_portable_profile(acc)
    cookie = str(acc.get("cookie_path") or "").strip()
    if not cookie:
        acc["cookie_path"] = default_cookie_path(aid)
    ensure_profile_directory_exists(aid, portable)


def ensure_profile_directory_exists(
    account_id: str,
    portable_path: str,
    *,
    project_root_dir: Path | None = None,
) -> None:
    """Import từ thư mục có sẵn — chỉ mkdir, không xóa dữ liệu cũ."""
    root = (project_root_dir or project_root()).resolve()
    profile_dir = resolve_account_path(root, portable_path)
    if profile_dir is None:
        raise ValueError(f"portable_path không hợp lệ: {portable_path!r}")
    profile_dir.mkdir(parents=True, exist_ok=True)
    _write_profile_owner_marker(profile_dir, account_id)


# Bỏ cache/lock khi copy profile ngoài → data/profiles (giữ cookie + lịch sử).
_IMPORT_SKIP_DIRS = frozenset(
    {
        "cache2",
        "startupcache",
        "thumbnails",
        "safebrowsing",
        "crashes",
        "minidumps",
        "datareporting",
        "saved-telemetry-pings",
        "shader-cache",
        "gpu-cache",
        "jumplistcache",
        "offlinecache",
        "__pycache__",
    }
)
_IMPORT_SKIP_FILES = frozenset({"parent.lock", "lock", ".parentlock"})


def _ignore_import_profile_junk(_directory: str, names: list[str]) -> set[str]:
    """Ignore callback cho ``shutil.copytree`` khi import profile ngoài."""
    skipped: set[str] = set()
    for name in names:
        folded = name.casefold()
        if folded in _IMPORT_SKIP_DIRS or folded in _IMPORT_SKIP_FILES:
            skipped.add(name)
    return skipped


def _cleanup_imported_firefox_locks(profile_dir: Path) -> None:
    """Xóa file khóa / session crash sau khi copy — tránh Firefox không mở được bản copy."""
    for name in (
        "parent.lock",
        "lock",
        ".parentlock",
        "compatibility.ini",
        "sessionstore.jsonlz4",
        "sessionstore.js",
        "sessionCheckpoints.json",
    ):
        target = profile_dir / name
        if target.is_file() or target.is_symlink():
            try:
                target.unlink()
            except OSError:
                pass


def import_external_profile_into_account(
    account_id: str,
    source_portable: str,
    browser_type: str = "firefox",
    *,
    project_root_dir: Path | None = None,
) -> str:
    """
    Copy thư mục profile ngoài (vd. ``D:\\…\\543677287``) vào ``data/profiles/<browser>/<id>``.

    Sau bước này tài khoản độc lập với thư mục nguồn — có thể xóa nguồn mà không mất phiên.

    Returns:
        ``portable_path`` relative trong dự án (vd. ``data/profiles/firefox/UID_543677287``).
    """
    root = (project_root_dir or project_root()).resolve()
    aid = str(account_id or "").strip()
    if not aid:
        raise ValueError("account_id rỗng — không import profile.")
    src = resolve_account_path(root, str(source_portable or "").strip())
    if src is None or not src.is_dir():
        raise ValueError(f"Thư mục profile nguồn không tồn tại: {source_portable!r}")

    # Firefox Portable: chỉ copy Data/profile (không copy cả App/).
    try:
        _display_root, profile_src, _exe = resolve_import_profile_source(src)
        src = profile_src
        logger.info(
            "Import profile: nguồn portable={} → profile thật={}",
            _display_root,
            src,
        )
    except ValueError:
        # Giữ src nếu đã là profile thuần / không phải layout portable.
        if not detect_browser_profile_kind(src):
            nested = resolve_firefox_portable_profile_dir(src)
            if nested is not None:
                src = nested

    dest_rel = default_portable_path(aid, browser_type)
    dest = resolve_account_path(root, dest_rel)
    if dest is None:
        raise ValueError(f"Không tạo được đường dẫn profile đích: {dest_rel!r}")

    profiles_root = (root / "data" / "profiles").resolve()
    try:
        dest.relative_to(profiles_root)
    except ValueError as exc:
        raise ValueError(f"Đích profile phải nằm trong data/profiles: {dest}") from exc

    src_res = src.resolve()
    dest_res = dest.resolve()
    if src_res == dest_res:
        ensure_profile_directory_exists(aid, dest_rel, project_root_dir=root)
        logger.info(
            "Import profile: nguồn đã nằm đúng đích account={} → {}",
            aid,
            dest_rel,
        )
        return dest_rel

    # Nguồn đã là profile trong project của cùng máy nhưng path khác id → vẫn copy.
    if dest.exists():
        logger.info("Import profile: xóa đích cũ trước khi copy | account={} | {}", aid, dest_res)
        safe_delete_path(dest, allowed_roots=[profiles_root], kind="dir", missing_ok=True)

    dest.parent.mkdir(parents=True, exist_ok=True)
    logger.info(
        "Import profile: copy {} → {} (account={})",
        src_res,
        dest_res,
        aid,
    )
    try:
        shutil.copytree(
            src_res,
            dest_res,
            ignore=_ignore_import_profile_junk,
            dirs_exist_ok=False,
        )
    except OSError as exc:
        raise RuntimeError(
            f"Không copy được profile từ:\n{src_res}\nsang:\n{dest_res}\n{exc}"
        ) from exc

    _cleanup_imported_firefox_locks(dest_res)
    _write_profile_owner_marker(dest_res, aid)
    if normalize_browser_storage(browser_type) == "firefox":
        try:
            from src.utils.firefox_portable_profile import ensure_firefox_portable_launcher

            ensure_firefox_portable_launcher(dest_res)
        except Exception as exc:  # noqa: BLE001
            logger.debug("Không ghi Mo_Firefox.bat (import): {}", exc)
    logger.info(
        "Import profile xong | account={} | portable={} | nguồn gốc có thể xóa an toàn: {}",
        aid,
        dest_rel,
        src_res,
    )
    return dest_rel


def detect_browser_profile_kind(profile_dir: Path | str) -> str:
    """
    Nhận loại profile trình duyệt từ nội dung thư mục.

    Returns:
        ``\"firefox\"`` | ``\"chromium\"`` | ``\"\"`` (không nhận ra).
    """
    pdir = Path(profile_dir)
    if not pdir.is_dir():
        return ""
    ff_hits = 0
    for name in _FIREFOX_PROFILE_MARKERS:
        try:
            if (pdir / name).is_file():
                ff_hits += 1
        except OSError:
            continue
    # profiles.ini / times.json cũng hay gặp ở root Firefox portable.
    try:
        if (pdir / "times.json").is_file():
            ff_hits += 1
    except OSError:
        pass
    if ff_hits >= 2:
        return "firefox"
    if ff_hits == 1 and any(
        (pdir / n).is_file() for n in ("prefs.js", "cookies.sqlite", "places.sqlite")
    ):
        return "firefox"

    cr_hits = 0
    for name in _CHROMIUM_PROFILE_MARKERS:
        try:
            if (pdir / name).is_file():
                cr_hits += 1
        except OSError:
            continue
    if cr_hits >= 1:
        return "chromium"
    return ""


def profile_dir_looks_importable(profile_dir: Path | str) -> bool:
    """True nếu thư mục giống profile Firefox/Chromium (có hoặc chưa có phiên)."""
    p = Path(profile_dir)
    if detect_browser_profile_kind(p):
        return True
    return resolve_firefox_portable_profile_dir(p) is not None


def is_firefox_portable_root(folder: Path | str) -> bool:
    """
    True nếu thư mục là bản Firefox Portable (PortableApps).

    Dấu hiệu: ``FirefoxPortable.exe`` và/hoặc cặp ``App`` + ``Data``.
    """
    root = Path(folder)
    if not root.is_dir():
        return False
    try:
        names = {p.name.casefold() for p in root.iterdir()}
    except OSError:
        return False
    has_exe = False
    try:
        for p in root.iterdir():
            if p.is_file() and p.name.casefold() in _FIREFOX_PORTABLE_EXE_NAMES:
                has_exe = True
                break
            if p.is_file() and p.suffix.casefold() == ".exe" and "firefox" in p.stem.casefold() and "portable" in p.stem.casefold():
                has_exe = True
                break
    except OSError:
        pass
    has_app = "app" in names
    has_data = "data" in names
    if has_exe and (has_app or has_data):
        return True
    if has_app and has_data:
        # Đủ App+Data kể cả thiếu .exe (đã bị xóa).
        return True
    return False


def _read_profiles_ini_path(ini_path: Path, *, base: Path) -> Path | None:
    """Đọc ``Path=`` / ``Default=1`` từ profiles.ini → thư mục profile tuyệt đối."""
    try:
        text = ini_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None
    blocks: list[dict[str, str]] = []
    cur: dict[str, str] = {}
    for line in text.splitlines():
        s = line.strip()
        if not s or s.startswith(";") or s.startswith("#"):
            continue
        if s.startswith("[") and s.endswith("]"):
            if cur:
                blocks.append(cur)
            cur = {"_section": s[1:-1]}
            continue
        if "=" in s:
            k, v = s.split("=", 1)
            cur[k.strip().lower()] = v.strip()
    if cur:
        blocks.append(cur)
    # Ưu tiên Default=1, rồi IsRelative + Path.
    ordered = sorted(
        blocks,
        key=lambda b: (
            0 if str(b.get("default", "")).strip() in ("1", "true", "yes") else 1,
            0 if "path" in b else 1,
        ),
    )
    for b in ordered:
        raw = str(b.get("path") or "").strip()
        if not raw:
            continue
        p = Path(raw)
        if not p.is_absolute():
            # Path trong profiles.ini thường relative tới thư mục chứa ini (Data/).
            p = (base / raw).resolve()
        else:
            p = p.resolve()
        if p.is_dir() and detect_browser_profile_kind(p):
            return p
        # Một số bản: Path=profile → file nằm trong Data/profile
        if (base / raw).is_dir() and detect_browser_profile_kind(base / raw):
            return (base / raw).resolve()
    return None


def resolve_firefox_portable_profile_dir(folder: Path | str) -> Path | None:
    """
    Từ root Firefox Portable (vd. ``…\\543677287``) → thư mục profile thật
    (thường ``Data\\profile`` hoặc path trong ``Data\\profiles.ini``).

    Returns:
        Path profile Firefox, hoặc None nếu không phải portable / không tìm thấy.
    """
    root = Path(folder)
    if not root.is_dir():
        return None
    # Đã là profile thuần → trả về chính nó.
    if detect_browser_profile_kind(root):
        return root.resolve()
    if not is_firefox_portable_root(root):
        # Có thể user chọn nhầm Data/ — thử lên cha hoặc xuống profile.
        data_guess = root / "profile"
        if detect_browser_profile_kind(data_guess):
            return data_guess.resolve()
        ini = root / "profiles.ini"
        if ini.is_file():
            hit = _read_profiles_ini_path(ini, base=root)
            if hit is not None:
                return hit
        return None

    data_dir = root / "Data"
    if not data_dir.is_dir():
        # Một số bản dùng data thường.
        for alt in ("data", "DATA"):
            if (root / alt).is_dir():
                data_dir = root / alt
                break
        else:
            return None

    # 1) profiles.ini
    for ini_name in ("profiles.ini", "Profiles.ini"):
        ini = data_dir / ini_name
        if ini.is_file():
            hit = _read_profiles_ini_path(ini, base=data_dir)
            if hit is not None:
                return hit

    # 2) Data/profile hoặc Data/profiles/<id>
    for cand in (
        data_dir / "profile",
        data_dir / "Profile",
        data_dir / "profiles" / "profile",
    ):
        if cand.is_dir() and detect_browser_profile_kind(cand):
            return cand.resolve()
    profiles_root = data_dir / "profiles"
    if profiles_root.is_dir():
        try:
            subs = sorted([p for p in profiles_root.iterdir() if p.is_dir()], key=lambda p: p.name.lower())
        except OSError:
            subs = []
        for sub in subs:
            if detect_browser_profile_kind(sub):
                return sub.resolve()

    # 3) Quét nông trong Data tìm prefs.js / cookies.sqlite
    try:
        for child in data_dir.iterdir():
            if not child.is_dir():
                continue
            if detect_browser_profile_kind(child):
                return child.resolve()
            for nested in child.iterdir() if child.is_dir() else []:
                if nested.is_dir() and detect_browser_profile_kind(nested):
                    return nested.resolve()
    except OSError:
        pass
    return None


def resolve_firefox_executable_for_playwright(raw_exe: str | Path | None) -> str:
    """
    Chuẩn hóa ``browser_exe_path`` trước khi đưa vào Playwright.

    ``FirefoxPortable.exe`` (PortableApps) → tìm ``App/**/firefox.exe`` cạnh nó;
    nếu không có thì trả ``\"\"`` để Playwright dùng Firefox bundle (hỗ trợ nhiều instance).
    """
    s = str(raw_exe or "").strip()
    if not s:
        return ""
    p = Path(s)
    try:
        if not p.is_file():
            return ""
    except OSError:
        return ""
    name = p.name.casefold()
    is_wrapper = name in _FIREFOX_PORTABLE_EXE_NAMES or (
        name.endswith(".exe") and "firefox" in name and "portable" in name
    )
    if not is_wrapper:
        # firefox.exe thật — OK cho multi-instance với -no-remote / Playwright.
        if name == "firefox.exe":
            return str(p.resolve())
        return str(p.resolve())

    # Wrapper PortableApps: đổi sang App/.../firefox.exe
    root = p.parent
    from_app = find_firefox_portable_exe(root)
    if from_app and Path(from_app).name.casefold() == "firefox.exe":
        return from_app
    # Tìm sâu hơn một chút trong App/
    app = root / "App"
    if app.is_dir():
        from src.utils.browser_exe_discover import find_browser_exe_in_directory

        found = find_browser_exe_in_directory(app, max_subdirs=200)
        if found and Path(found).name.casefold() == "firefox.exe":
            return found
    logger.warning(
        "FirefoxPortable.exe không có App\\\\firefox.exe kèm theo — bỏ exe, dùng Playwright Firefox."
    )
    return ""


def find_firefox_portable_exe(folder: Path | str) -> str:
    """
    Tìm ``firefox.exe`` trong root Firefox Portable.

    Ưu tiên ``App/**/firefox.exe`` — **không** ưu tiên ``FirefoxPortable.exe``
    (wrapper PortableApps chặn «Another instance of Firefox is already running»
    kể cả khi ToolFB/Playwright đang mở profile khác).
    """
    root = Path(folder)
    if not root.is_dir():
        return ""
    app = root / "App"
    if app.is_dir():
        from src.utils.browser_exe_discover import find_browser_exe_in_directory

        found = find_browser_exe_in_directory(app)
        if found and Path(found).name.casefold() == "firefox.exe":
            return found
        if found and "portable" not in Path(found).stem.casefold():
            return found
    # Chỉ dùng FirefoxPortable.exe khi không có firefox.exe thật.
    try:
        for p in root.iterdir():
            if p.is_file() and (
                p.name.casefold() in _FIREFOX_PORTABLE_EXE_NAMES
                or (
                    p.suffix.casefold() == ".exe"
                    and "firefox" in p.stem.casefold()
                    and "portable" in p.stem.casefold()
                )
            ):
                logger.warning(
                    "Chỉ thấy {} — dễ bị chặn multi-instance. Nên dùng App\\\\…\\\\firefox.exe.",
                    p.name,
                )
                return str(p.resolve())
    except OSError:
        pass
    return ""


def resolve_import_profile_source(folder: Path | str) -> tuple[Path, Path, str]:
    """
    Chuẩn hóa thư mục user chọn → (display_root, profile_dir, browser_exe).

    - Firefox Portable ``543677287`` → display=543677287, profile=Data/profile, exe=FirefoxPortable.exe
    - Profile thuần → cả hai cùng path
    """
    root = Path(folder).resolve()
    exe = ""
    if is_firefox_portable_root(root):
        prof = resolve_firefox_portable_profile_dir(root)
        exe = find_firefox_portable_exe(root)
        if prof is None:
            raise ValueError(
                f"Thư mục giống Firefox Portable nhưng không tìm thấy profile trong Data\\:\n{root}\n"
                "Kiểm tra Data\\profile hoặc Data\\profiles.ini."
            )
        return root, prof, exe
    # User chọn nhầm App/ hoặc Data/ trong portable → leo lên root portable.
    parent = root.parent
    if root.name.casefold() in {"app", "data", "other", "profile", "profiles"} and is_firefox_portable_root(parent):
        prof = resolve_firefox_portable_profile_dir(parent)
        exe = find_firefox_portable_exe(parent)
        if prof is not None:
            return parent, prof, exe
    if detect_browser_profile_kind(root):
        return root, root, find_firefox_portable_exe(root) or find_firefox_portable_exe(parent)
    # Portable lồng: cha là portable
    if is_firefox_portable_root(parent):
        prof = resolve_firefox_portable_profile_dir(parent)
        if prof is not None:
            return parent, prof, find_firefox_portable_exe(parent)
    raise ValueError(
        f"Không nhận ra profile Firefox/Chromium hoặc Firefox Portable tại:\n{root}"
    )


def list_importable_profile_dirs(selected: Path | str) -> list[Path]:
    """
    Chọn thư mục profile để import.

    - Firefox Portable (``App``+``Data``+``FirefoxPortable.exe``) → **1** mục = root portable
      (không tách App/Data thành 2 tài khoản).
    - Profile Firefox/Chromium thuần → [selected].
    - Thư mục cha chứa nhiều UID/portable → các thư mục con hợp lệ.
    """
    root = Path(selected)
    if not root.is_dir():
        return []
    try:
        root = root.resolve()
    except OSError:
        return []

    # 1) Chính selected là Firefox Portable hoặc profile thuần.
    if is_firefox_portable_root(root) or detect_browser_profile_kind(root):
        return [root]

    # 2) Selected là App/Data nhầm → trả về root portable cha.
    if root.name.casefold() in {"app", "data", "other", "profile"} and is_firefox_portable_root(root.parent):
        return [root.parent.resolve()]

    found: list[Path] = []
    try:
        children = sorted(
            [p for p in root.iterdir() if p.is_dir()],
            key=lambda p: p.name.lower(),
        )
    except OSError:
        return []
    for child in children:
        name_l = child.name.strip().lower()
        if name_l in _PROFILE_SCAN_SKIP_NAMES or name_l.startswith("."):
            continue
        if is_firefox_portable_root(child) or detect_browser_profile_kind(child):
            found.append(child.resolve())
            continue
        # Con là Data/profile sâu hơn một cấp (hiếm).
        nested = resolve_firefox_portable_profile_dir(child)
        if nested is not None and is_firefox_portable_root(child):
            found.append(child.resolve())
    return found


def suggest_account_id_from_profile_dir(profile_dir: Path | str) -> tuple[str, str]:
    """
    Gợi ý ``(account_id, facebook_uid)`` từ tên thư mục profile.

    Thư mục toàn số (UID FB) → id ``UID_<số>``, facebook_uid = số.
    Với Firefox Portable: dùng tên root (``543677287``), không dùng ``App``/``Data``.
    """
    p = Path(profile_dir)
    name = p.name.strip()
    # Nếu đang trỏ Data/profile → lấy tên portable root (UID).
    if name.casefold() in {"profile", "profiles", "data", "app"} or detect_browser_profile_kind(p):
        for up in (p, p.parent, p.parent.parent, p.parent.parent.parent):
            try:
                if is_firefox_portable_root(up):
                    name = up.name.strip()
                    break
                if up.name.isdigit() and len(up.name) >= 5:
                    name = up.name.strip()
                    break
            except Exception:
                continue
    digits = name if name.isdigit() and len(name) >= 5 else ""
    if digits:
        return f"UID_{digits}", digits
    # Không lấy App/Data làm id.
    if name.casefold() in {"app", "data", "other", "profile", "profiles"}:
        name = p.parent.name.strip() if p.parent else name
        if name.isdigit() and len(name) >= 5:
            return f"UID_{name}", name
    safe = re.sub(r"[^\w\-]+", "_", name)[:48].strip("_") if name else ""
    if not safe or safe.casefold() in {"app", "data", "other"}:
        safe = f"acc_{uuid.uuid4().hex[:10]}"
    elif safe[0].isdigit():
        safe = f"acc_{safe}"
    return safe, ""


def portable_profile_likely_has_session(
    profile_path: str,
    *,
    project_root_dir: Path | None = None,
) -> bool:
    """
    True nếu thư mục profile portable có dấu hiệu đã dùng đăng nhập (cookie DB / nhiều file).

    Dùng trước khi mở browser — không đọc ``c_user`` trực tiếp từ SQLite.
    """
    root = (project_root_dir or project_root()).resolve()
    pdir = resolve_account_path(root, str(profile_path or "").strip())
    if pdir is None or not pdir.is_dir():
        return False
    for pattern in ("cookies.sqlite", "**/cookies.sqlite", "Default/Cookies", "Cookies"):
        try:
            hit = next(pdir.glob(pattern), None)
            if hit is not None and hit.is_file() and hit.stat().st_size > 64:
                return True
        except OSError:
            continue
    try:
        places = next(pdir.glob("places.sqlite"), None)
        if places is not None and places.is_file() and places.stat().st_size > 1024:
            return True
    except OSError:
        pass
    return False


def resolve_account_portable_profile(acc: dict[str, Any]) -> str:
    """
    Gắn đúng ``portable_path`` — ưu tiên thư mục **đã có lịch sử/phiên**, không dùng profile trống mới mkdir.

    Gọi trước ``launch_persistent_context`` để mỗi lần mở lại đúng profile cũ của TK.
    """
    from src.utils.account_proxy_mapper import enrich_account_dict_from_registry

    enrich_account_dict_from_registry(acc)
    root = project_root().resolve()
    aid = str(acc.get("registry_id") or acc.get("id") or "").strip() or "manual"
    bt = normalize_browser_storage(str(acc.get("browser_type") or "firefox"))

    configured = str(acc.get("portable_path") or acc.get("profile_path") or "").strip()
    configured_dir: Path | None = None
    if configured:
        configured_dir = resolve_account_path(root, configured)

    # Đã có trong accounts.json + profile đã có phiên/lịch sử → dùng đúng acc_…
    if (
        acc.get("registry_id")
        and configured_dir is not None
        and configured_dir.is_dir()
        and portable_profile_likely_has_session(str(configured_dir))
    ):
        rel = relativize_account_storage_path(str(configured_dir), project_root_dir=root)
        acc["portable_path"] = rel
        acc["profile_path"] = rel
        logger.info(
            "[Profile] Dùng profile registry (có phiên) account={} (hiển thị={}) → {}",
            aid,
            acc.get("display_account_id") or acc.get("id"),
            rel,
        )
        _sync_resolved_profile_to_registry(acc, rel)
        return rel

    best = pick_best_profile_dir_for_account(acc, project_root_dir=root)
    if best is not None:
        best_score = _score_profile_dir(best, acc)[0]
        cfg_score = _score_profile_dir(configured_dir, acc)[0] if configured_dir and configured_dir.is_dir() else -1
        if configured_dir is None or not configured_dir.is_dir() or best_score > cfg_score:
            if configured_dir is not None and configured_dir.is_dir() and best.resolve() != configured_dir.resolve():
                logger.warning(
                    "[Profile] Chuyển sang profile có lịch sử account={} | cũ={} (score {}) → mới={} (score {})",
                    aid,
                    configured,
                    cfg_score,
                    best,
                    best_score,
                )
            rel = relativize_account_storage_path(str(best), project_root_dir=root)
            acc["portable_path"] = rel
            acc["profile_path"] = rel
            _sync_resolved_profile_to_registry(acc, rel)
            return rel

    if configured_dir is not None and configured_dir.is_dir():
        rel = relativize_account_storage_path(str(configured_dir), project_root_dir=root)
        acc["portable_path"] = rel
        acc["profile_path"] = rel
        logger.debug("[Profile] portable từ registry/cấu hình account={} → {}", aid, rel)
        return rel

    rel = default_portable_path(aid, bt)
    acc["portable_path"] = rel
    acc["profile_path"] = rel
    return rel


def _sync_resolved_profile_to_registry(acc: dict[str, Any], portable_rel: str) -> None:
    """Ghi lại portable_path đã khôi phục vào accounts.json để lần sau không mở nhầm profile trống."""
    aid = str(acc.get("registry_id") or acc.get("id") or "").strip()
    if not aid or not portable_rel:
        return
    try:
        from src.utils.db_manager import AccountsDatabaseManager

        db = AccountsDatabaseManager()
        rows = db.load_all()
        rec = next((r for r in rows if str(r.get("id") or "") == aid), None)
        if not rec:
            fb = str(acc.get("facebook_uid") or "").strip()
            if fb.isdigit():
                rec = next((r for r in rows if str(r.get("facebook_uid") or "").strip() == fb), None)
        if not rec:
            return
        cur = str(rec.get("portable_path") or rec.get("profile_path") or "").strip()
        if cur.replace("\\", "/") == portable_rel.replace("\\", "/"):
            return
        db.update_account_fields(
            aid,
            {"portable_path": portable_rel, "profile_path": portable_rel},
        )
        logger.info("[Profile] Đã sync portable_path registry account={} → {}", aid, portable_rel)
    except Exception as exc:  # noqa: BLE001
        logger.debug("[Profile] sync registry {}: {}", aid, exc)


def _write_profile_owner_marker(profile_dir: Path, account_id: str) -> None:
    """Ghi marker chủ sở hữu profile (không ghi đè nếu đã thuộc tài khoản khác)."""
    aid = str(account_id or "").strip()
    if not aid:
        return
    marker = profile_dir / ".toolfb_account_id"
    if marker.is_file():
        owner = marker.read_text(encoding="utf-8").strip()
        if owner and owner != aid:
            raise ValueError(
                f"Thư mục profile {profile_dir} đã thuộc tài khoản «{owner}», "
                f"không gán cho «{aid}». Mỗi tài khoản phải có profile riêng."
            )
        return
    marker.write_text(aid + "\n", encoding="utf-8")


def assert_profile_directory_owned_by(
    account_id: str,
    profile_dir: Path,
    *,
    create_marker_if_missing: bool = True,
    account: dict[str, Any] | None = None,
) -> None:
    """
    Đảm bảo thư mục profile chỉ dùng cho một ``account_id`` (file ``.toolfb_account_id``).
    """
    aid = str(account_id or "").strip()
    if not aid or not profile_dir.is_dir():
        return
    marker = profile_dir / ".toolfb_account_id"
    if not marker.is_file():
        if create_marker_if_missing:
            _write_profile_owner_marker(profile_dir, str((account or {}).get("registry_id") or aid))
        return
    owner = marker.read_text(encoding="utf-8").strip()
    if account and _profile_owner_matches_account(owner, account):
        return
    if owner and owner != aid:
        raise ValueError(
            f"Profile {profile_dir} thuộc tài khoản «{owner}», không mở cho «{aid}». "
            "Hai tài khoản không được dùng chung một thư mục profile."
        )


def _account_id_variants_for_storage(account: dict[str, Any]) -> list[str]:
    """Mọi khóa có thể trỏ tới cùng profile/cookie trên đĩa (UID vs acc_…)."""
    seen: set[str] = set()
    out: list[str] = []

    def _add(raw: str) -> None:
        s = str(raw or "").strip()
        if not s or s in seen:
            return
        seen.add(s)
        out.append(s)

    _add(str(account.get("registry_id") or ""))
    _add(str(account.get("id") or ""))
    _add(str(account.get("display_account_id") or ""))
    fb = str(account.get("facebook_uid") or "").strip()
    if fb.isdigit():
        _add(fb)
        _add(f"UID_{fb}")
    aid = str(account.get("id") or "").strip()
    if aid.upper().startswith("UID_"):
        num = aid.split("_", 1)[-1]
        if num.isdigit():
            _add(num)
    try:
        from src.utils.db_manager import AccountsDatabaseManager

        for rec in AccountsDatabaseManager().load_all():
            rid = str(rec.get("id") or "").strip()
            rfb = str(rec.get("facebook_uid") or "").strip()
            if not rid:
                continue
            linked = {rid}
            if rfb.isdigit():
                linked.add(rfb)
                linked.add(f"UID_{rfb}")
            if seen & linked:
                for x in linked:
                    _add(x)
    except Exception:
        pass
    return out


def _profile_owner_matches_account(owner: str, account: dict[str, Any]) -> bool:
    """True nếu marker ``.toolfb_account_id`` thuộc cùng tài khoản (UID_ / acc_ / registry)."""
    o = str(owner or "").strip()
    if not o:
        return False
    return o in set(_account_id_variants_for_storage(account))


def _score_profile_dir(profile_dir: Path, account: dict[str, Any] | str) -> tuple[int, float]:
    """
    Điểm profile: có cookie DB / lịch sử > có marker đúng chủ > nhiều file > mới tạo.
    """
    variants = (
        {str(account).strip()}
        if isinstance(account, str)
        else set(_account_id_variants_for_storage(account))
    )
    score = 0
    if portable_profile_likely_has_session(str(profile_dir)):
        score += 200
    marker = profile_dir / ".toolfb_account_id"
    if marker.is_file():
        owner = marker.read_text(encoding="utf-8").strip()
        if owner and owner in variants:
            score += 80
        elif owner:
            score += 10
    try:
        entries = [e for e in profile_dir.iterdir() if e.name != ".toolfb_account_id"]
        score += min(len(entries), 25)
        mtime = profile_dir.stat().st_mtime
    except OSError:
        mtime = 0.0
    return score, mtime


def pick_best_profile_dir_for_account(
    account: dict[str, Any],
    *,
    project_root_dir: Path | None = None,
) -> Path | None:
    """Chọn thư mục profile tốt nhất trong các ứng viên (ưu tiên đã có phiên đăng nhập)."""
    root = (project_root_dir or project_root()).resolve()
    candidates = iter_profile_dirs_for_account(account, project_root_dir=root)
    if not candidates:
        return None
    scored = [(_score_profile_dir(p, account), p) for p in candidates]
    scored.sort(key=lambda item: (item[0][0], item[0][1]), reverse=True)
    return scored[0][1]


def iter_profile_dirs_for_account(
    account: dict[str, Any],
    *,
    project_root_dir: Path | None = None,
) -> list[Path]:
    """Mọi thư mục profile có thể gắn với tài khoản (đường dẫn tuyệt đối, không trùng)."""
    root = (project_root_dir or project_root()).resolve()
    seen: set[Path] = set()
    out: list[Path] = []
    reg_portable = str(account.get("portable_path") or account.get("profile_path") or "").strip()
    reg_path = resolve_account_path(root, reg_portable) if reg_portable else None
    reg_id = str(account.get("registry_id") or account.get("id") or "").strip()
    skip_uid_guess = bool(
        reg_id.startswith("acc_")
        and reg_path is not None
        and reg_path.is_dir()
        and portable_profile_likely_has_session(str(reg_path))
        and "profiles/firefox/acc_" in reg_portable.replace("\\", "/")
    )
    for key in ("portable_path", "profile_path"):
        pdir = resolve_account_path(root, str(account.get(key) or ""))
        if pdir is not None and pdir.is_dir() and pdir not in seen:
            seen.add(pdir)
            out.append(pdir)
    for aid in _account_id_variants_for_storage(account):
        if skip_uid_guess and aid.upper().startswith("UID_"):
            continue
        for sub in _BROWSER_DIRS:
            pdir = resolve_account_path(root, default_portable_path(aid, sub))
            if pdir is not None and pdir.is_dir() and pdir not in seen:
                seen.add(pdir)
                out.append(pdir)
    return out


def delete_account_browser_bundle(
    account: dict[str, Any],
    *,
    project_root_dir: Path | None = None,
) -> list[str]:
    """
    Xóa profile Firefox/Playwright, cookie và vault của một tài khoản.

    Returns:
        Danh sách đường dẫn đã xóa (hoặc cố gắng xóa).
    """
    root = (project_root_dir or project_root()).resolve()
    aid = str(account.get("id", "")).strip()
    deleted: list[str] = []
    profiles_root = (root / "data" / "profiles").resolve()
    cookies_root = (root / "data" / "cookies").resolve()

    for pdir in iter_profile_dirs_for_account(account, project_root_dir=root):
        if safe_delete_path(pdir, allowed_roots=[profiles_root], kind="dir", missing_ok=True):
            deleted.append(str(pdir))
            logger.info("Đã xóa profile Playwright | account={} | {}", aid, pdir)

    ck = resolve_account_path(root, str(account.get("cookie_path") or ""))
    if ck is not None and ck.is_file():
        if safe_delete_path(ck, allowed_roots=[cookies_root], kind="file", missing_ok=True):
            deleted.append(str(ck))

    cookie_default = cookies_root / f"{aid}.json"
    if cookie_default.is_file() and str(cookie_default.resolve()) not in deleted:
        if safe_delete_path(cookie_default, allowed_roots=[cookies_root], kind="file", missing_ok=True):
            deleted.append(str(cookie_default))

    try:
        from src.utils.account_credentials import delete_account_credentials

        if delete_account_credentials(aid):
            deleted.append(f"vault:account:{aid}")
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không xóa vault credentials account={}: {}", aid, exc)

    return deleted
