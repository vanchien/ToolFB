"""Xuất và gộp dữ liệu ToolFB giữa hai máy. Không xóa dữ liệu máy đang dùng."""

from __future__ import annotations

import json
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from collections.abc import Callable
from typing import Any


BUNDLE_TYPE = "toolfb_data_bundle"
BUNDLE_VERSION = 3
_COOKIE_MAX_BYTES = 1_500_000
SETTINGS_FILES = (
    "human_interaction_settings.json",
    "facebook_groups.json",
    "entities.json",
    "schedule.json",
    "page_creation.json",
    "business_managers.json",
    "page_insights.json",
    "ai_image_config.json",
    "universal_video_downloader.json",
    "app_secrets.json",
    "auto_update.json",
    "ai_styles.json",
    "ai_video_styles.json",
    "video_styles.json",
    "goal_registry.json",
)


@dataclass
class SyncReport:
    """Kết quả gộp. Máy đích không bị xóa bản ghi."""

    accounts_added: int = 0
    accounts_filled: int = 0
    accounts_kept: int = 0
    pages_added: int = 0
    pages_filled: int = 0
    pages_kept: int = 0
    jobs_added: int = 0
    jobs_kept: int = 0
    credentials_filled: int = 0
    cookies_copied: int = 0
    cookies_kept: int = 0
    profiles_copied: int = 0
    profiles_kept: int = 0
    profiles_missing: int = 0
    settings_updated: int = 0
    notes: list[str] = field(default_factory=list)

    def summary(self) -> str:
        """Tóm tắt cho hộp thoại."""
        lines = [
            f"Tài khoản mới: {self.accounts_added}",
            f"Tài khoản đã có, bổ sung chỗ trống: {self.accounts_filled}",
            f"Tài khoản giữ nguyên: {self.accounts_kept}",
            f"Page mới: {self.pages_added}",
            f"Page đã có, bổ sung chỗ trống: {self.pages_filled}",
            f"Page giữ nguyên: {self.pages_kept}",
            f"Lịch mới: {self.jobs_added}",
            f"Cookie chép thêm: {self.cookies_copied}",
            f"Cookie máy này được giữ: {self.cookies_kept}",
            f"Thư mục profile được chép: {self.profiles_copied}",
            f"Profile máy này đã đăng nhập, được giữ: {self.profiles_kept}",
            f"Profile không thấy trong gói: {self.profiles_missing}",
            f"File cài đặt được bổ sung: {self.settings_updated}",
            "Không xóa tài khoản, page hay cài đặt đang có trên máy này.",
        ]
        if self.notes:
            lines.append("")
            lines.extend(self.notes[:8])
        return "\n".join(lines)


def account_match_key(row: dict[str, Any]) -> str:
    """Khóa trùng tài khoản: UID số, rồi email, rồi id."""
    uid = "".join(ch for ch in str(row.get("facebook_uid") or "") if ch.isdigit())
    if len(uid) >= 5:
        return f"uid:{uid}"
    email = str(row.get("email") or "").strip().casefold()
    if "@" in email:
        return f"mail:{email}"
    return f"id:{str(row.get('id') or '').strip()}"


def page_match_key(row: dict[str, Any]) -> str:
    """Khóa trùng page: id Facebook, hoặc tài khoản + URL."""
    fb = "".join(ch for ch in str(row.get("fb_page_id") or "") if ch.isdigit())
    if len(fb) >= 5:
        return f"fb:{fb}"
    aid = str(row.get("account_id") or "").strip()
    url = str(row.get("page_url") or "").strip().rstrip("/").casefold()
    return f"url:{aid}:{url}"


def relativize_storage_path(path: str) -> str:
    """Đổi đường dẫn máy cũ thành data/profiles hoặc data/cookies nếu nhận ra."""
    raw = str(path or "").strip()
    folded = raw.replace("\\", "/")
    lower = folded.casefold()
    for marker in ("/data/profiles/", "/data/cookies/"):
        index = lower.find(marker)
        if index >= 0:
            return folded[index + 1 :]
    return raw


def _blank(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, dict):
        return not any(str(item or "").strip() for item in value.values())
    if isinstance(value, list):
        return len(value) == 0
    return False


def _fill_blanks(local: dict[str, Any], incoming: dict[str, Any]) -> bool:
    """Điền trường đang trống. Giữ giá trị máy này nếu cả hai đều có."""
    changed = False
    for key, value in incoming.items():
        if key not in local or _blank(local.get(key)):
            if _blank(value):
                continue
            local[key] = value
            changed = True
    return changed


def merge_accounts(
    local_rows: list[dict[str, Any]],
    incoming_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, str], SyncReport]:
    """Gộp tài khoản. Trả danh sách mới, map id gói → id máy này, và báo cáo."""
    report = SyncReport()
    merged = [dict(row) for row in local_rows if isinstance(row, dict)]
    by_key: dict[str, dict[str, Any]] = {}
    for row in merged:
        by_key[account_match_key(row)] = row
    id_map: dict[str, str] = {}
    for raw in incoming_rows:
        if not isinstance(raw, dict):
            continue
        incoming = dict(raw)
        incoming_id = str(incoming.get("id") or "").strip()
        for path_key in ("portable_path", "profile_path", "cookie_path"):
            if incoming.get(path_key):
                incoming[path_key] = relativize_storage_path(str(incoming[path_key]))
        if incoming.get("portable_path") and not incoming.get("profile_path"):
            incoming["profile_path"] = incoming["portable_path"]
        key = account_match_key(incoming)
        current = by_key.get(key)
        if current is None:
            if not incoming_id:
                report.notes.append("Bỏ một tài khoản thiếu id.")
                continue
            merged.append(incoming)
            by_key[key] = incoming
            id_map[incoming_id] = incoming_id
            report.accounts_added += 1
            continue
        local_id = str(current.get("id") or incoming_id)
        if incoming_id:
            id_map[incoming_id] = local_id
        if _fill_blanks(current, incoming):
            report.accounts_filled += 1
    local_count = sum(1 for row in local_rows if isinstance(row, dict))
    report.accounts_kept = max(0, local_count - report.accounts_filled)
    return merged, id_map, report


def merge_pages(
    local_rows: list[dict[str, Any]],
    incoming_rows: list[dict[str, Any]],
    account_id_map: dict[str, str],
) -> tuple[list[dict[str, Any]], dict[str, str], SyncReport]:
    """Gộp page. Giữ page máy này. Đổi account_id theo tài khoản đã khớp."""
    report = SyncReport()
    merged = [dict(row) for row in local_rows if isinstance(row, dict)]
    by_key = {page_match_key(row): row for row in merged}
    page_id_map: dict[str, str] = {}
    for raw in incoming_rows:
        if not isinstance(raw, dict):
            continue
        incoming = dict(raw)
        aid = str(incoming.get("account_id") or "")
        incoming["account_id"] = account_id_map.get(aid, aid)
        incoming_page_id = str(incoming.get("id") or "").strip()
        key = page_match_key(incoming)
        current = by_key.get(key)
        if current is None:
            if not str(incoming.get("page_name") or "").strip() or not str(incoming.get("page_url") or "").strip():
                report.notes.append("Bỏ một page thiếu tên hoặc link.")
                continue
            if not str(incoming.get("id") or "").strip():
                report.notes.append(f"Bỏ page {incoming.get('page_name')} vì thiếu id.")
                continue
            merged.append(incoming)
            by_key[key] = incoming
            if incoming_page_id:
                page_id_map[incoming_page_id] = incoming_page_id
            report.pages_added += 1
            continue
        local_page_id = str(current.get("id") or incoming_page_id)
        if incoming_page_id:
            page_id_map[incoming_page_id] = local_page_id
        if _fill_blanks(current, incoming):
            report.pages_filled += 1
    local_count = sum(1 for row in local_rows if isinstance(row, dict))
    report.pages_kept = max(0, local_count - report.pages_filled)
    return merged, page_id_map, report


def merge_jobs(
    local_rows: list[dict[str, Any]],
    incoming_rows: list[dict[str, Any]],
    account_id_map: dict[str, str],
    page_id_map: dict[str, str],
) -> tuple[list[dict[str, Any]], SyncReport]:
    """Thêm lịch chưa có. Lịch cùng id trên máy này được giữ."""
    report = SyncReport()
    merged = [dict(row) for row in local_rows if isinstance(row, dict)]
    seen = {str(row.get("id") or "") for row in merged}
    report.jobs_kept = len(merged)
    for raw in incoming_rows:
        if not isinstance(raw, dict):
            continue
        incoming = dict(raw)
        job_id = str(incoming.get("id") or "").strip()
        if not job_id or job_id in seen:
            continue
        aid = str(incoming.get("account_id") or "")
        pid = str(incoming.get("page_id") or "")
        incoming["account_id"] = account_id_map.get(aid, aid)
        incoming["page_id"] = page_id_map.get(pid, pid)
        merged.append(incoming)
        seen.add(job_id)
        report.jobs_added += 1
    return merged, report


def merge_credentials(
    local_store: dict[str, Any],
    incoming_store: dict[str, Any],
    account_id_map: dict[str, str],
) -> tuple[dict[str, Any], int]:
    """Gộp vault mật khẩu. Không ghi đè mật khẩu máy này đang có."""
    local = dict(local_store) if isinstance(local_store, dict) else {"accounts": {}}
    accounts = dict(local.get("accounts") or {})
    incoming_accounts = (incoming_store or {}).get("accounts") if isinstance(incoming_store, dict) else {}
    if not isinstance(incoming_accounts, dict):
        incoming_accounts = {}
    filled = 0
    for raw_id, payload in incoming_accounts.items():
        if not isinstance(payload, dict):
            continue
        target_id = account_id_map.get(str(raw_id), str(raw_id))
        current = accounts.get(target_id)
        if not isinstance(current, dict):
            accounts[target_id] = dict(payload)
            filled += 1
            continue
        if _fill_blanks(current, payload):
            filled += 1
        accounts[target_id] = current
    local["accounts"] = accounts
    return local, filled


def bundle_from_project(project_root: Path) -> dict[str, Any]:
    """Đọc tài khoản, page, lịch, vault và cookie từ một thư mục ToolFB."""
    root = Path(project_root)

    def _read_list(name: str) -> list[dict[str, Any]]:
        path = root / "config" / name
        if not path.is_file():
            return []
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, list):
            raise ValueError(f"{name} phải là mảng.")
        return [row for row in raw if isinstance(row, dict)]

    cred_path = root / "config" / "account_credentials.json"
    credentials: dict[str, Any] = {"accounts": {}}
    if cred_path.is_file():
        raw_cred = json.loads(cred_path.read_text(encoding="utf-8"))
        if isinstance(raw_cred, dict):
            credentials = raw_cred
    return build_bundle(
        accounts=_read_list("accounts.json"),
        pages=_read_list("pages.json"),
        jobs=_read_list("schedule_posts.json"),
        credentials=credentials,
        project_root=root,
    )


def read_bundle(path: Path) -> dict[str, Any]:
    """Đọc gói JSON. Thiếu lịch hoặc vault thì coi là rỗng."""
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("Gói dữ liệu phải là object JSON.")
    data = raw.get("data")
    if not isinstance(data, dict):
        data = raw
    accounts = data.get("accounts")
    pages = data.get("pages")
    jobs = data.get("schedule_posts") if isinstance(data.get("schedule_posts"), list) else []
    if not isinstance(accounts, list) or not isinstance(pages, list):
        raise ValueError("Gói cần có mảng accounts và pages.")
    credentials = data.get("credentials") if isinstance(data.get("credentials"), dict) else {"accounts": {}}
    cookies = data.get("cookies") if isinstance(data.get("cookies"), list) else []
    return {
        "accounts": accounts,
        "pages": pages,
        "schedule_posts": jobs,
        "credentials": credentials,
        "cookies": cookies,
    }


def build_bundle(
    *,
    accounts: list[dict[str, Any]],
    pages: list[dict[str, Any]],
    jobs: list[dict[str, Any]],
    credentials: dict[str, Any] | None = None,
    project_root: Path | None = None,
) -> dict[str, Any]:
    """Tạo gói mang sang máy khác. Kèm file cookie nhỏ nếu nằm trong project."""
    from datetime import datetime

    exported_accounts: list[dict[str, Any]] = []
    for row in accounts:
        if not isinstance(row, dict):
            continue
        item = dict(row)
        for path_key in ("portable_path", "profile_path", "cookie_path"):
            if item.get(path_key):
                item[path_key] = relativize_storage_path(str(item[path_key]))
        if item.get("portable_path") and not item.get("profile_path"):
            item["profile_path"] = item["portable_path"]
        exported_accounts.append(item)
    cookies: list[dict[str, str]] = []
    if project_root is not None:
        root = Path(project_root).resolve()
        for row in accounts:
            rel = relativize_storage_path(str(row.get("cookie_path") or ""))
            if not rel or ".." in rel.replace("\\", "/"):
                continue
            source = (root / rel).resolve()
            try:
                source.relative_to(root)
            except ValueError:
                continue
            if not source.is_file() or source.stat().st_size > _COOKIE_MAX_BYTES:
                continue
            try:
                cookie_text = source.read_text(encoding="utf-8")
            except OSError:
                continue
            cookies.append({"relative_path": rel.replace("\\", "/"), "text": cookie_text})
    settings = _read_settings(Path(project_root)) if project_root is not None else {}
    return {
        "bundle_type": BUNDLE_TYPE,
        "bundle_version": BUNDLE_VERSION,
        "exported_at": datetime.now().replace(microsecond=0).isoformat(),
        "project": "ToolFB",
        "data": {
            "accounts": exported_accounts,
            "pages": pages,
            "schedule_posts": jobs,
            "credentials": credentials or {"accounts": {}},
            "cookies": cookies,
            "settings": settings,
        },
    }


def _read_settings(project_root: Path) -> dict[str, Any]:
    """Đọc các file cài đặt được phép mang sang máy khác."""
    found: dict[str, Any] = {}
    root = Path(project_root)
    for name in SETTINGS_FILES:
        path = root / "config" / name
        if not path.is_file() or path.stat().st_size > 8_000_000:
            continue
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(raw, (dict, list)):
            found[name] = raw
    return found


def _merge_json_list(local: list[Any], incoming: list[Any]) -> bool:
    """Thêm phần tử chưa có. Bản ghi có id thì so theo id."""
    seen = {str(item.get("id")) for item in local if isinstance(item, dict) and item.get("id")}
    changed = False
    for item in incoming:
        if isinstance(item, dict) and item.get("id"):
            key = str(item.get("id"))
            if key in seen:
                continue
            local.append(item)
            seen.add(key)
            changed = True
            continue
        if item not in local:
            local.append(item)
            changed = True
    return changed


def _merge_json_value(local: Any, incoming: Any) -> tuple[Any, bool]:
    """Giữ giá trị máy này. Chỉ điền chỗ trống hoặc thêm id mới."""
    if _blank(local):
        return incoming, not _blank(incoming)
    if isinstance(local, dict) and isinstance(incoming, dict):
        changed = False
        for key, value in incoming.items():
            if key not in local or _blank(local.get(key)):
                if _blank(value):
                    continue
                local[key] = value
                changed = True
                continue
            merged, sub = _merge_json_value(local[key], value)
            local[key] = merged
            changed = changed or sub
        return local, changed
    if isinstance(local, list) and isinstance(incoming, list):
        return local, _merge_json_list(local, incoming)
    return local, False


def apply_settings(project_root: Path, incoming: dict[str, Any], *, write: bool = True) -> int:
    """Gộp file cài đặt. Không ghi đè giá trị máy này đang có."""
    if not isinstance(incoming, dict):
        return 0
    root = Path(project_root).resolve()
    config_dir = root / "config"
    updated = 0
    for name, payload in incoming.items():
        if name not in SETTINGS_FILES or not isinstance(payload, (dict, list)):
            continue
        target = (config_dir / name).resolve()
        try:
            target.relative_to(config_dir.resolve())
        except ValueError:
            continue
        current: Any = None
        if target.is_file():
            try:
                current = json.loads(target.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                current = None
        merged, changed = _merge_json_value(current, payload)
        if not changed:
            continue
        if write:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(json.dumps(merged, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        updated += 1
    return updated


def plan_merge(
    *,
    local_accounts: list[dict[str, Any]],
    local_pages: list[dict[str, Any]],
    local_jobs: list[dict[str, Any]],
    local_credentials: dict[str, Any],
    bundle: dict[str, Any],
) -> tuple[dict[str, Any], SyncReport]:
    """Tính dữ liệu sau khi gộp. Chưa ghi đĩa."""
    accounts, id_map, account_report = merge_accounts(local_accounts, list(bundle.get("accounts") or []))
    pages, page_map, page_report = merge_pages(local_pages, list(bundle.get("pages") or []), id_map)
    jobs, job_report = merge_jobs(local_jobs, list(bundle.get("schedule_posts") or []), id_map, page_map)
    credentials, cred_filled = merge_credentials(
        local_credentials,
        dict(bundle.get("credentials") or {}),
        id_map,
    )
    report = SyncReport(
        accounts_added=account_report.accounts_added,
        accounts_filled=account_report.accounts_filled,
        accounts_kept=account_report.accounts_kept,
        pages_added=page_report.pages_added,
        pages_filled=page_report.pages_filled,
        pages_kept=page_report.pages_kept,
        jobs_added=job_report.jobs_added,
        jobs_kept=job_report.jobs_kept,
        credentials_filled=cred_filled,
        notes=account_report.notes + page_report.notes,
    )
    return {
        "accounts": accounts,
        "pages": pages,
        "schedule_posts": jobs,
        "credentials": credentials,
        "cookies": list(bundle.get("cookies") or []),
        "settings": dict(bundle.get("settings") or {}) if isinstance(bundle.get("settings"), dict) else {},
        "profile_jobs": profile_transfer_jobs(list(bundle.get("accounts") or []), accounts, id_map),
    }, report


_SKIP_PROFILE_DIRS = {
    "cache",
    "cache2",
    "code cache",
    "gpucache",
    "dawncache",
    "grshadercache",
    "shadercache",
    "startupcache",
    "crashpad",
    "browsermetrics",
    "thumbnails",
    "safebrowsing",
}
_SKIP_PROFILE_FILES = {"parent.lock", "lock", ".parentlock"}


def profile_has_login(path: Path) -> bool:
    """Profile đã có cookie nên không cần đăng nhập lại."""
    if not path.is_dir():
        return False
    markers = (
        path / "cookies.sqlite",
        path / "cookies.sqlite-wal",
        path / "Default" / "Cookies",
        path / "Default" / "Network" / "Cookies",
    )
    for marker in markers:
        try:
            if marker.is_file() and marker.stat().st_size > 0:
                return True
        except OSError:
            continue
    return False


def _safe_profile_path(root: Path, relative: str) -> Path | None:
    """Chỉ cho đường dẫn nằm trong data/profiles của đúng thư mục gốc."""
    rel = str(relative or "").replace("\\", "/").lstrip("/")
    parts = [part for part in rel.split("/") if part]
    if not parts or ".." in parts or parts[0] != "data" or len(parts) < 3 or parts[1] != "profiles":
        return None
    root_resolved = Path(root).resolve()
    target = root_resolved.joinpath(*parts).resolve()
    try:
        target.relative_to(root_resolved)
    except ValueError:
        return None
    return target


def _ignore_profile_junk(_directory: str, names: list[str]) -> set[str]:
    """Bỏ cache và file khóa Firefox. Giữ cookie và phiên đăng nhập."""
    skipped: set[str] = set()
    for name in names:
        folded = name.casefold()
        if folded in _SKIP_PROFILE_DIRS or folded in _SKIP_PROFILE_FILES:
            skipped.add(name)
    return skipped


def profile_transfer_jobs(
    incoming_accounts: list[dict[str, Any]],
    merged_accounts: list[dict[str, Any]],
    account_id_map: dict[str, str],
) -> list[dict[str, str]]:
    """Nối profile trong gói với thư mục profile sẽ dùng trên máy này."""
    by_id = {str(row.get("id") or ""): row for row in merged_accounts if isinstance(row, dict)}
    jobs: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for raw in incoming_accounts:
        if not isinstance(raw, dict):
            continue
        source = relativize_storage_path(str(raw.get("portable_path") or raw.get("profile_path") or ""))
        incoming_id = str(raw.get("id") or "").strip()
        target_id = account_id_map.get(incoming_id, incoming_id)
        dest_row = by_id.get(target_id) or {}
        dest = relativize_storage_path(str(dest_row.get("portable_path") or dest_row.get("profile_path") or ""))
        if not dest:
            dest = source
        if not source.startswith("data/profiles/") or not dest.startswith("data/profiles/"):
            continue
        key = (dest, source)
        if key in seen:
            continue
        seen.add(key)
        jobs.append({"dest": dest, "source": source})
    return jobs


def export_profiles(
    project_root: Path,
    accounts: list[dict[str, Any]],
    package_dir: Path,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> tuple[int, int]:
    """Chép thư mục profile vào gói. Bỏ cache. Trả số thư mục đã chép và số thư mục thiếu."""
    rows = [row for row in accounts if isinstance(row, dict)]
    total = len(rows)
    copied = 0
    missing = 0
    for index, row in enumerate(rows, start=1):
        rel = relativize_storage_path(str(row.get("portable_path") or row.get("profile_path") or ""))
        label = str(row.get("name") or row.get("id") or rel or index)
        if on_progress is not None:
            on_progress(index, total, label)
        source = _safe_profile_path(project_root, rel)
        dest = _safe_profile_path(package_dir, rel)
        if source is None or dest is None or not source.is_dir():
            missing += 1
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(source, dest, dirs_exist_ok=True, ignore=_ignore_profile_junk)
        copied += 1
    return copied, missing


def profile_install_action(source_root: Path, dest_root: Path, job: dict[str, str]) -> str:
    """Một profile: copied, kept hoặc missing. Chưa ghi đĩa."""
    source = _safe_profile_path(source_root, str(job.get("source") or ""))
    dest = _safe_profile_path(dest_root, str(job.get("dest") or ""))
    if source is None or dest is None or not source.is_dir():
        return "missing"
    try:
        if source.resolve() == dest.resolve():
            return "kept"
    except OSError:
        return "missing"
    if profile_has_login(dest):
        return "kept"
    return "copied"


def preview_profile_install(
    source_root: Path,
    dest_root: Path,
    jobs: list[dict[str, str]],
) -> tuple[int, int, int]:
    """Đếm profile sẽ chép, giữ, hoặc thiếu. Dùng cho hộp xác nhận."""
    copied = kept = missing = 0
    for job in jobs:
        action = profile_install_action(source_root, dest_root, job)
        if action == "copied":
            copied += 1
        elif action == "kept":
            kept += 1
        else:
            missing += 1
    return copied, kept, missing


def install_profiles(
    source_root: Path,
    dest_root: Path,
    jobs: list[dict[str, str]],
    on_progress: Callable[[int, int, str], None] | None = None,
) -> tuple[int, int, int]:
    """Chép profile còn thiếu phiên đăng nhập. Profile máy này đã có cookie thì giữ nguyên."""
    copied = 0
    kept = 0
    missing = 0
    total = len(jobs)
    for index, job in enumerate(jobs, start=1):
        if on_progress is not None:
            on_progress(index, total, str(job.get("dest") or index))
        action = profile_install_action(source_root, dest_root, job)
        if action == "kept":
            kept += 1
            continue
        if action == "missing":
            missing += 1
            continue
        source = _safe_profile_path(source_root, str(job.get("source") or ""))
        dest = _safe_profile_path(dest_root, str(job.get("dest") or ""))
        if source is None or dest is None:
            missing += 1
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(source, dest, dirs_exist_ok=True, ignore=_ignore_profile_junk)
        copied += 1
    return copied, kept, missing


def write_missing_cookies(project_root: Path, cookies: list[dict[str, Any]]) -> tuple[int, int]:
    """Chép cookie chưa có. File cookie máy này đang có thì giữ nguyên."""
    root = Path(project_root).resolve()
    copied = 0
    kept = 0
    for item in cookies:
        if not isinstance(item, dict):
            continue
        rel = str(item.get("relative_path") or "").replace("\\", "/").lstrip("/")
        text = item.get("text")
        if not rel.startswith("data/cookies/") or ".." in rel or not isinstance(text, str):
            continue
        target = (root / rel).resolve()
        try:
            target.relative_to(root)
        except ValueError:
            continue
        if target.is_file() and target.stat().st_size > 0:
            kept += 1
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8")
        copied += 1
    return copied, kept
