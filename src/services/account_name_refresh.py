"""
Cập nhật tên Facebook theo tài khoản — tuần tự, có thể unit/E2E test.

Không mở browser ở đây: GUI/provider inject ``lookup_cookie`` / ``read_browser``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Iterable

from src.utils.schedule_posts_filters import (
    account_ids_for_name_refresh,
    account_name_is_placeholder,
    account_name_should_take_facebook_name,
)

LookupFn = Callable[[dict[str, Any]], tuple[str, str]]
BrowserFn = Callable[[str], tuple[str, str]]
SaveFn = Callable[[str, str], None]
ProgressFn = Callable[[int, int, str, str], None]
SessionReadyFn = Callable[[dict[str, Any]], bool]


@dataclass
class NameRefreshResult:
    """Kết quả một đợt cập nhật tên."""

    updated: list[tuple[str, str]] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    forced: bool = False
    total: int = 0


def should_apply_discovered_name(
    current: str,
    found: str,
    account_id: str,
    *,
    forced: bool,
) -> bool:
    """Quyết định ghi tên mới: placeholder → luôn; tick cưỡng bức → đổi nếu tên hợp lệ."""
    discovered = " ".join(str(found or "").split())
    now = " ".join(str(current or "").split())
    aid = str(account_id or "").strip()
    if not discovered:
        return False
    if account_name_should_take_facebook_name(now, discovered, aid):
        return True
    if not forced:
        return False
    if account_name_is_placeholder(discovered, aid):
        return False
    if discovered.isdigit():
        return False
    return discovered.casefold() != now.casefold()


def plan_name_refresh_ids(
    rows: Iterable[dict[str, Any]],
    ticked_ids: Iterable[str] | None = None,
) -> tuple[list[str], bool]:
    """Id cần lấy tên + cờ forced (có tick)."""
    return account_ids_for_name_refresh(rows, ticked_ids)


def run_name_refresh_sequential(
    rows: list[dict[str, Any]],
    account_ids: list[str],
    *,
    forced: bool,
    lookup_cookie: LookupFn,
    read_browser: BrowserFn,
    save_name: SaveFn,
    on_progress: ProgressFn | None = None,
    close_open_profiles: Callable[[list[str]], None] | None = None,
    session_ready: SessionReadyFn | None = None,
) -> NameRefreshResult:
    """
    Lần lượt từng tài khoản: cookie → (nếu cần) 1 browser → lưu → báo tiến độ.

    ``close_open_profiles`` chạy một lần trước vòng lặp (đóng «Mở profile» đang giữ Firefox).
    ``session_ready``: False → bỏ qua mở browser (tránh treo Firefox khi chưa đăng nhập).
    """
    by_id = {str(r.get("id") or "").strip(): r for r in rows if str(r.get("id") or "").strip()}
    ids = [str(a).strip() for a in account_ids if str(a).strip()]
    result = NameRefreshResult(forced=forced, total=len(ids))
    if not ids:
        return result

    if close_open_profiles is not None:
        try:
            close_open_profiles(ids)
        except Exception:
            pass

    total = len(ids)
    for index, aid in enumerate(ids, start=1):
        if on_progress is not None:
            on_progress(index, total, aid, "start")
        acc = by_id.get(aid)
        if not isinstance(acc, dict):
            result.skipped.append(f"{aid}: không tìm thấy bản ghi")
            continue

        found, reason = lookup_cookie(acc)
        used_browser = False
        if not found:
            if session_ready is not None:
                try:
                    ready = bool(session_ready(acc))
                except Exception:
                    ready = True
                if not ready:
                    result.skipped.append(
                        f"{aid}: profile chưa có phiên — «Mở profile» đăng nhập rồi cập nhật tên"
                    )
                    if on_progress is not None:
                        on_progress(index, total, aid, "skip")
                    continue
            if on_progress is not None:
                on_progress(index, total, aid, "browser")
            found, reason = read_browser(aid)
            used_browser = True

        current = str(acc.get("name") or "")
        if not should_apply_discovered_name(current, found, aid, forced=forced):
            tag = reason or ("giữ tên hiện tại" if found else "không lấy được tên")
            if used_browser and not found:
                tag = reason or "mở browser nhưng chưa đọc được tên"
            result.skipped.append(f"{aid}: {tag}")
            if on_progress is not None:
                on_progress(index, total, aid, "skip")
            continue

        try:
            save_name(aid, found)
        except Exception as exc:  # noqa: BLE001
            result.skipped.append(f"{aid}: không lưu được ({exc})")
            if on_progress is not None:
                on_progress(index, total, aid, "error")
            continue

        result.updated.append((aid, found))
        # Đồng bộ bản ghi in-memory để vòng sau thấy tên mới.
        acc["name"] = found
        if on_progress is not None:
            on_progress(index, total, aid, "done")

    return result


def assert_sequential_browser_opens(open_log: list[str]) -> None:
    """E2E helper: mọi lần mở browser phải xen kẽ đóng — không chồng 2 aid cùng lúc."""
    if not open_log:
        return
    # open_log dạng ["open:a", "close:a", "open:b", ...]
    open_stack: list[str] = []
    for event in open_log:
        kind, _, aid = event.partition(":")
        if kind == "open":
            assert not open_stack, f"Mở chồng: đang mở {open_stack} khi open {aid}"
            open_stack.append(aid)
        elif kind == "close":
            assert open_stack and open_stack[-1] == aid, f"Đóng lệch: {event} stack={open_stack}"
            open_stack.pop()
    assert not open_stack, f"Còn browser chưa đóng: {open_stack}"
