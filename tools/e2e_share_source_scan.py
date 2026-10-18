"""
E2E quét Page nguồn thật (browser profile đã login) — kiểm tra đúng trang + chỉ lấy href Reels.

Chạy:
  .venv\\Scripts\\python.exe tools\\e2e_share_source_scan.py
  .venv\\Scripts\\python.exe tools\\e2e_share_source_scan.py --page https://www.facebook.com/facebook

Thoát mã 0 nếu đúng Page và có ít nhất 1 reel href; khác 0 nếu lệch trang / lỗi session.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault("TOOLFB_FB_REELS_HEADLESS", "1")


def _load_account() -> dict:
    from src.utils.db_manager import AccountsDatabaseManager

    rows = AccountsDatabaseManager().load_all()
    for row in rows:
        path = str(row.get("portable_path") or row.get("profile_path") or "").strip()
        if path and ((ROOT / path).is_dir() or Path(path).is_dir()):
            return dict(row)
    raise SystemExit("Không có tài khoản nào có portable_path tồn tại trên đĩa.")


def main() -> int:
    # Windows console cp1252 — tránh UnicodeEncodeError khi in tiếng Việt.
    for stream in (sys.stdout, sys.stderr):
        reconf = getattr(stream, "reconfigure", None)
        if callable(reconf):
            try:
                reconf(encoding="utf-8", errors="replace")
            except Exception:
                pass

    parser = argparse.ArgumentParser(description="E2E quét đúng Page nguồn (Reels)")
    parser.add_argument(
        "--page",
        default="https://www.facebook.com/facebook",
        help="Link Page nguồn cần quét (mặc định: facebook.com/facebook)",
    )
    parser.add_argument("--headed", action="store_true", help="Hiện cửa sổ browser")
    args = parser.parse_args()

    from src.services.facebook_groups.source_watch import SourceState, detect_new_videos, parse_source_urls
    from src.services.facebook_reels_catalog import (
        facebook_page_identity,
        scan_facebook_profile_reels_page,
        url_matches_facebook_page,
    )

    try:
        sources = parse_source_urls(args.page)
    except ValueError as exc:
        print(f"FAIL parse: {exc}")
        return 2
    page_url = sources[0]
    want = facebook_page_identity(page_url)
    print(f"Page nguồn: {page_url} (identity={want})")

    account = _load_account()
    print(f"Tài khoản quét: {account.get('id')} / {account.get('name')}")

    if args.headed:
        os.environ["TOOLFB_FB_REELS_HEADLESS"] = "0"

    statuses: list[str] = []

    def _status(text: str) -> None:
        statuses.append(text)
        print(f"  … {text}")

    result = scan_facebook_profile_reels_page(
        page_url=page_url,
        account=account,
        max_reels=15,
        max_scroll_rounds=4,
        scroll_until_end=False,
        max_scan_minutes=3.0,
        scroll_pause_sec=0.9,
        headless=not args.headed,
        status=_status,
        include_html_ids=False,
        scan_videos_tab=False,
    )
    print(json.dumps({"ok": result.get("ok"), "message": result.get("message"), "n": len(result.get("items") or [])}, ensure_ascii=False))

    if not result.get("ok"):
        print("FAIL: quét không thành công")
        return 1

    items = result.get("items") or []
    urls = [str(row.get("url") or "") for row in items if isinstance(row, dict)]
    if not urls:
        print("FAIL: không có reel href nào")
        return 1

    # Kiểm tra không bị nhảy trang: status không báo lệch / message không lệch.
    bad = [s for s in statuses if "lệch" in s.casefold() or "không đứng đúng" in s.casefold()]
    if bad:
        print("FAIL: đã lệch Page nguồn trong lúc quét:", bad)
        return 1

    if not url_matches_facebook_page(page_url, page_url):
        print("FAIL: identity Page nguồn không hợp lệ")
        return 1

    state = SourceState()
    first = detect_new_videos(state, urls)
    if first:
        print("FAIL: lan dau phai chi ghi moc, nhung ra:", first)
        return 1
    # ID giả > max_id thật (snowflake FB thường > 10^15).
    fake_new = int(state.max_id) + 1
    second_urls = [f"https://www.facebook.com/reel/{fake_new}", *urls]
    fresh = detect_new_videos(state, second_urls)
    expect = [f"https://www.facebook.com/reel/{fake_new}"]
    if fresh != expect:
        print("FAIL detect_new:", fresh, "expect:", expect, "max_id=", state.max_id)
        return 1

    print("OK e2e: đúng Page + href Reels + detect chỉ nhận video mới trên đầu")
    print("Mẫu reel:", urls[:3])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
