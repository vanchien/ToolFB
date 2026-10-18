"""
Chạy thử end to end: tải TikTok / YouTube / Facebook → Video Editor (chỉnh nhanh + xuất) → job chờ đăng
→ chia Page → tạo lịch Facebook (chỉ kiểm tra dữ liệu, KHÔNG đăng thật).

Mọi dữ liệu ghi vào thư mục tạm, không đụng metadata thật của ToolFB.

Chạy:  .venv\\Scripts\\python.exe tools\\e2e_download_edit_pending.py
"""

from __future__ import annotations

import copy
import json
import sys
import tempfile
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from loguru import logger  # noqa: E402

from src.services.job_post_runtime import validate_page_for_post_job, validate_queue_job_payload  # noqa: E402
from src.services.universal_video_downloader import (  # noqa: E402
    DownloadMetadataStore,
    UniversalVideoDownloader,
    annotate_downloaded_library_row,
    downloaded_file_empty_reason,
    list_videos_for_download_job,
    unique_playable_download_rows,
)
from src.services.video_editor.ffmpeg_builder import (  # noqa: E402
    FFmpegCommandBuilder,
    clip_export_skip_reason,
    exported_mp4_reject_reason,
)
from src.services.video_editor.media_manager import MediaManager  # noqa: E402
from src.services.video_editor.page_split import apply_page_split, pages_already_used  # noqa: E402
from src.services.video_editor.pending_import import (  # noqa: E402
    JOB_STATUS_IMPORTED,
    JOB_STATUS_PARTIAL,
    fb_page_owner_error,
    job_status_after_import,
    respace_account_schedules,
    schedule_split_chunks,
    select_items_for_schedule,
)
from src.utils.schedule_batch_preview import parse_daily_time_windows  # noqa: E402
from src.utils.schedule_posts_manager import SchedulePostsManager  # noqa: E402
from src.services.video_editor.project_manager import VideoEditorProjectManager  # noqa: E402
from src.services.video_editor.render_worker import RenderWorker  # noqa: E402
from src.services.video_editor.timeline_manager import TimelineManager, iter_video_clips  # noqa: E402
from src.utils.ffmpeg_paths import resolve_ffmpeg_executable  # noqa: E402

SOURCES = {
    "tiktok": "https://www.tiktok.com/@aicat012/video/7394862241427787026",
    "youtube": "https://www.youtube.com/watch?v=jNQXAC9IVRw",
    "facebook": "https://www.facebook.com/reel/1195289147628387",
}

RESULTS: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> bool:
    """Ghi kết quả một bước kiểm tra."""
    RESULTS.append((name, bool(ok), detail))
    (logger.success if ok else logger.error)("{} {} {}", "OK  " if ok else "LỖI", name, detail)
    return bool(ok)


def _ve_paths(root: Path) -> dict[str, Path]:
    """Thư mục Video Editor tạm."""
    names = ("projects", "media", "stock_audio", "temp", "renders", "thumbnails", "waveforms",
             "subtitles", "presets", "templates", "logs")
    d = {"root": root, **{n: root / n for n in names}}
    for p in d.values():
        p.mkdir(parents=True, exist_ok=True)
    return d


def _clip_only_project(src: dict[str, Any], clip_id: str) -> dict[str, Any] | None:
    """Project chỉ còn một clip video (như xuất từng video trong GUI)."""
    out = copy.deepcopy(src)
    found = None
    for tr in out.get("tracks") or []:
        if not isinstance(tr, dict):
            continue
        if str(tr.get("type") or "") == "video":
            keep = [c for c in tr.get("clips") or [] if isinstance(c, dict) and str(c.get("id")) == clip_id]
            tr["clips"] = keep
            if keep:
                found = keep[0]
        else:
            tr["clips"] = []
    if found is None:
        return None
    found["timeline_start"] = 0.0
    out["duration"] = max(0.1, float(found.get("duration") or 0.1))
    return out


def step_download(work: Path) -> tuple[DownloadMetadataStore, dict[str, str]]:
    """Tải một video mỗi nền tảng, mỗi nền tảng một job (như GUI)."""
    dl_root = work / "downloader"
    dl_root.mkdir(parents=True)
    paths = {
        "root": dl_root,
        "jobs_file": dl_root / "download_jobs.json",
        "videos_file": dl_root / "downloaded_videos.json",
        "archive": dl_root / "archive.txt",
        "scan_lists_file": dl_root / "scan_lists.json",
    }
    for k in ("jobs_file", "videos_file"):
        paths[k].write_text("[]\n", encoding="utf-8")
    paths["archive"].write_text("", encoding="utf-8")
    store = DownloadMetadataStore(paths=paths)
    out_dir = work / "taivideo"
    out_dir.mkdir()
    job_ids: dict[str, str] = {}
    with patch("src.services.universal_video_downloader.ensure_downloader_layout", return_value=paths):
        down = UniversalVideoDownloader(log=lambda m: logger.info("[tải] {}", m))
        down._store = store
        for plat, url in SOURCES.items():
            job = down.create_download_job(
                url,
                {
                    "platform": plat,
                    "url_type": "single_video",
                    "max_videos": 1,
                    "output_dir": str(out_dir),
                    "organize_by_platform": True,
                    "organize_by_uploader": True,
                    "skip_existing": False,
                    "write_info_json": True,
                    "write_thumbnail": False,
                },
            )
            jid = str(job["id"])
            job_ids[plat] = jid
            try:
                res = down.run_download_job(jid)
            except Exception as exc:  # noqa: BLE001
                check(f"tải {plat}", False, f"ngoại lệ: {exc}")
                continue
            rows = list_videos_for_download_job(jid, job=store.get_job(jid), store=store)
            good = [r for r in rows if r.get("status") == "downloaded"]
            status = str((store.get_job(jid) or {}).get("status") or res.get("status") or "")
            if good:
                r0 = good[0]
                vp = Path(str(r0.get("video_path") or ""))
                check(
                    f"tải {plat}",
                    vp.is_file() and not downloaded_file_empty_reason(vp) and float(r0.get("duration") or 0) >= 0.5,
                    f"{vp.name} · {float(r0.get('duration') or 0):.1f}s · job {status}",
                )
            else:
                err = str((store.get_job(jid) or {}).get("error_message") or "")[:200]
                bad_reported_ok = status == "completed" and not (store.get_job(jid) or {}).get("failed_items")
                check(f"tải {plat}", False, f"không có video dùng được · job {status} · {err}")
                check(f"tải {plat} lỗi không báo thành công", not bad_reported_ok, f"status={status}")
        down._close_tiktok_browser()
    return store, job_ids


def step_editor_and_export(work: Path, store: DownloadMetadataStore) -> list[dict[str, Any]]:
    """Nạp tất cả video (bỏ trùng) → chỉnh nhanh → xuất từng video → item job chờ đăng."""
    rows = [annotate_downloaded_library_row(r) for r in store.list_downloaded_videos()]
    if rows:
        dup = dict(rows[0])
        dup["id"] = "src_video_dup"
        dup["download_job_id"] = "dl_dup_test"
        rows.append(dup)
    unique, n_dup = unique_playable_download_rows(rows)
    check("bỏ video trùng giữa các job", n_dup >= 1 and len(unique) == len(rows) - 1 - sum(
        1 for r in rows if r.get("status") == "lỗi"
    ), f"{len(rows)} dòng → {len(unique)} video, bỏ {n_dup} bản trùng")
    if not unique:
        return []

    ffmpeg_bin = resolve_ffmpeg_executable()
    if not check("có ffmpeg", bool(ffmpeg_bin), str(ffmpeg_bin or "")):
        return []
    paths = _ve_paths(work / "video_editor")
    pm = VideoEditorProjectManager(paths=paths)
    mm = MediaManager(paths=paths)
    tm = TimelineManager(project_manager=pm)
    project = pm.create_project("e2e", width=1080, height=1920, fps=30)
    meta_by_mid: dict[str, dict[str, Any]] = {}
    for r in unique:
        media = mm.import_media(str(r["video_path"]), "video", copy_to_library=False)
        media["source_download_video_id"] = str(r.get("id") or "")
        project.setdefault("media", []).append(media)
        meta_by_mid[str(media["id"])] = r
        tm.add_clip(project, str(media["id"]), "video", persist=False, recompute_duration=False)
    clips = iter_video_clips(project)
    check("đưa hết lên timeline", len(clips) == len(unique), f"{len(clips)} clip")

    for cl in clips:
        cid = str(cl["id"])
        tm.update_clip(
            project,
            cid,
            {"flip_horizontal": True, "flip_vertical": False, "rotation": 0, "canvas_mode": "fit", "reverse": False},
            persist=False,
            recompute_duration=False,
        )
        ss = float(cl.get("source_start") or 0)
        se = float(cl.get("source_end") or 0)
        if se - ss > 2.5:
            tm.trim_clip(project, cid, ss + 0.5, se - 0.5, persist=False, recompute_duration=False)
    tm.refresh_project_duration(project)
    pm.save_project(project)

    out_dir = paths["renders"] / "job_e2e"
    out_dir.mkdir(parents=True)
    items: list[dict[str, Any]] = []
    worker = RenderWorker()
    for idx, cl in enumerate(iter_video_clips(project), start=1):
        cid = str(cl["id"])
        media = next(m for m in project["media"] if str(m["id"]) == str(cl["media_id"]))
        src_path = Path(str(media.get("path") or ""))
        skip = clip_export_skip_reason(media, src_path, float(cl.get("duration") or 0))
        if skip:
            check(f"xuất clip {idx}", False, skip)
            continue
        bproj = _clip_only_project(project, cid)
        out_p = out_dir / f"{idx:02d}_{cid}.mp4"
        dur = float(bproj["duration"])
        cmd = FFmpegCommandBuilder().build_export_command(
            bproj,
            str(out_p),
            ffmpeg_bin=ffmpeg_bin,
            encoding_overrides={"preset": "veryfast", "crf": 18},
            lightweight_mode_override=False,
        )
        res = worker.render(bproj, str(out_p), cmd, duration_sec=dur)
        bad = exported_mp4_reject_reason(out_p, expect_w=1080, expect_h=1920, expect_dur=dur) if res.get("ok") else ""
        ok = bool(res.get("ok")) and not bad
        src = meta_by_mid.get(str(media["id"])) or {}
        check(
            f"xuất clip {idx} ({src.get('platform')})",
            ok,
            bad or str(res.get("error_message") or "")[:200] or f"{out_p.name} · {out_p.stat().st_size // 1024} KB",
        )
        if ok:
            items.append(
                {
                    "video_path": str(out_p.resolve()),
                    "title": str(src.get("title") or out_p.stem)[:120],
                    "content": str(src.get("title") or out_p.stem)[:120],
                    "hashtags": list(src.get("hashtags") or []),
                    "source_download_video_id": str(src.get("id") or ""),
                }
            )
    return items


def step_pending_and_schedule(items: list[dict[str, Any]]) -> None:
    """Job chờ đăng → chia Page → nạp lịch chỉ vào fanpage đã chọn."""
    if not items:
        check("job chờ đăng", False, "không có video xuất được")
        return
    job = {
        "id": f"expjob_{uuid.uuid4().hex[:10]}",
        "job_name": "e2e",
        "status": "saved",
        "publish_target": "unspecified",
        "items": items,
    }
    acc = "acc_e2e"
    pages = {
        "pg_a": {"id": "pg_a", "account_id": acc, "page_name": "Fanpage A", "page_url": "https://www.facebook.com/a"},
        "pg_b": {"id": "pg_b", "account_id": acc, "page_name": "Fanpage B", "page_url": "https://www.facebook.com/b"},
        "pg_other": {"id": "pg_other", "account_id": "acc_khac", "page_name": "Page lạ", "page_url": "https://x"},
    }
    n = len(items)
    first_dest = [{"account_id": acc, "page_id": "pg_a"}]
    if n >= 2:
        res = apply_page_split(job["items"], first_dest, 1)
        check("chia Page lần 1 (1 video/page)", res["assigned"] == 1, f"còn {len(res['leftover_indexes'])} video chưa có Page")
    else:
        apply_page_split(job["items"], first_dest, None)

    picked = select_items_for_schedule(job["items"])
    check(
        "nạp lịch chỉ lấy clip đã có Page",
        len(picked["entries"]) == (1 if n >= 2 else n) and picked["waiting_page"] == max(0, n - 1 if n >= 2 else 0),
        f"nạp {len(picked['entries'])} · chờ Page {picked['waiting_page']}",
    )
    schedule_rows: list[dict[str, Any]] = []

    def _schedule(entries: list[tuple[int, dict[str, Any]]]) -> None:
        for _i, it in entries:
            aid = str(it["item_preset_fb_account_id"])
            pid = str(it["item_preset_fb_page_id"])
            err = fb_page_owner_error(pages.get(pid), aid, pid)
            check(f"Page {pid} thuộc tài khoản", not err, err)
            row = {
                "id": f"sched_{uuid.uuid4().hex[:10]}",
                "page_id": pid,
                "account_id": aid,
                "post_type": "video",
                "media_files": [it["video_path"]],
                "video_path": it["video_path"],
                "status": "pending",
            }
            validate_queue_job_payload(row)
            validate_page_for_post_job(pages[pid], aid)
            schedule_rows.append(row)
            it["item_scheduled_platform"] = "facebook"
            it["item_scheduled_job_id"] = row["id"]

    _schedule(picked["entries"])
    status1 = job_status_after_import(job["items"])
    check(
        "job còn clip chưa nạp → trạng thái partial",
        status1 == (JOB_STATUS_PARTIAL if n >= 2 else JOB_STATUS_IMPORTED),
        status1,
    )

    if n >= 2:
        used = pages_already_used(job["items"])
        res2 = apply_page_split(
            job["items"],
            [{"account_id": acc, "page_id": "pg_b"}],
            None,
            only_unassigned=True,
        )
        check("lần sau chia phần còn lại cho Page khác", res2["assigned"] == n - 1 and "pg_a" in used, f"gán {res2['assigned']} cho pg_b")
        picked2 = select_items_for_schedule(job["items"])
        check("nạp lần 2 không trùng clip đã có lịch", picked2["skipped_scheduled"] == 1 and len(picked2["entries"]) == n - 1)
        _schedule(picked2["entries"])
        check("job nạp đủ → imported", job_status_after_import(job["items"]) == JOB_STATUS_IMPORTED)

    fresh = [{k: v for k, v in it.items() if not k.startswith("item_")} for it in items]
    split = apply_page_split(
        fresh,
        [{"account_id": acc, "page_id": "pg_a"}, {"account_id": acc, "page_id": "pg_b"}],
        None,
    )
    auto_rows = schedule_split_chunks(
        fresh,
        job,
        split["chunks"],
        start_date=date.today(),
        windows=parse_daily_time_windows("08:00-10:00, 14:00-16:00, 20:00-22:00"),
        per_day=3,
        tz_name="Asia/Ho_Chi_Minh",
    )
    for row in auto_rows:
        validate_queue_job_payload(row)
        validate_page_for_post_job(pages[row["page_id"]], row["account_id"])
    check(
        "chia + lên lịch luôn (3 video/ngày mỗi Page)",
        len(auto_rows) == n and job_status_after_import(fresh) == JOB_STATUS_IMPORTED,
        ", ".join(f"{r['page_id']} {r['slot_base_local']}" for r in auto_rows),
    )

    step_account_gap_and_clear_fields(fresh, job, pages, acc, auto_rows)

    dest_pages = {r["page_id"] for r in schedule_rows}
    check("chỉ đăng lên fanpage đã chỉ định", dest_pages <= {"pg_a", "pg_b"}, ", ".join(sorted(dest_pages)))
    check("mỗi video một lịch, không trùng", len({r["video_path"] for r in schedule_rows}) == len(schedule_rows) == n)
    check("Page của tài khoản khác bị chặn", bool(fb_page_owner_error(pages["pg_other"], acc, "pg_other")))


def _min_gap_minutes(rows: list[dict[str, Any]]) -> float:
    """Khoảng cách nhỏ nhất (phút) giữa hai lịch liên tiếp của cùng tài khoản."""
    by_acc: dict[str, list[datetime]] = {}
    for r in rows:
        by_acc.setdefault(str(r["account_id"]), []).append(datetime.fromisoformat(str(r["scheduled_at"])))
    gaps = [
        (b - a).total_seconds() / 60
        for ts in by_acc.values()
        for a, b in zip(sorted(ts), sorted(ts)[1:])
    ]
    return min(gaps) if gaps else 999.0


def step_account_gap_and_clear_fields(
    items: list[dict[str, Any]],
    job: dict[str, Any],
    pages: dict[str, dict[str, Any]],
    acc: str,
    auto_rows: list[dict[str, Any]],
) -> None:
    """Các Page cùng tài khoản cách nhau ≥ 2 phút; giãn lịch cũ; xóa tiêu đề/hashtag hàng loạt."""
    check("lịch chia luôn: cùng tài khoản cách ≥ 2 phút", _min_gap_minutes(auto_rows) >= 2, f"min {_min_gap_minutes(auto_rows):.1f} phút")

    many_pages = {
        f"pg_{i}": {
            "id": f"pg_{i}",
            "account_id": acc,
            "page_name": f"Fanpage {i}",
            "page_url": f"https://www.facebook.com/pg{i}",
        }
        for i in range(10)
    }
    pages.update(many_pages)
    clones = [dict(items[i % len(items)]) for i in range(20)]
    for it in clones:
        for k in list(it):
            if k.startswith("item_"):
                it.pop(k)
    split = apply_page_split(clones, [{"account_id": acc, "page_id": p} for p in many_pages], None)
    tight = schedule_split_chunks(
        clones,
        job,
        split["chunks"],
        start_date=date.today() + timedelta(days=1),
        windows=parse_daily_time_windows("09:00-09:10"),
        per_day=1,
        tz_name="Asia/Ho_Chi_Minh",
        existing_rows=auto_rows,
    )
    check(
        "10 Page cùng tài khoản, khung 10 phút → vẫn cách ≥ 2 phút",
        len(tight) == 20 and _min_gap_minutes(tight + auto_rows) >= 2,
        f"min {_min_gap_minutes(tight + auto_rows):.1f} phút",
    )

    t0 = datetime.now(timezone.utc).replace(second=0, microsecond=0) + timedelta(days=3)
    legacy = [
        {"id": f"old_{i}", "account_id": acc, "page_id": f"pg_{i}", "status": "pending",
         "scheduled_at": t0.isoformat(), "post_type": "video", "media_files": [items[0]["video_path"]],
         "title": "Công ty", "content": "Công ty", "hashtags": ["#a", "#b"]}
        for i in range(6)
    ]
    changed = respace_account_schedules(legacy)
    check("giãn lịch cũ trùng giờ", len(changed) == 5 and _min_gap_minutes(legacy) >= 2, f"dời {len(changed)} lịch")

    with tempfile.TemporaryDirectory(prefix="toolfb_e2e_sched_") as tmp:
        mgr = SchedulePostsManager(Path(tmp) / "schedule_posts.json")
        mgr.upsert_many(legacy)
        ids = [r["id"] for r in legacy]
        mgr.update_jobs_fields_sequential(ids, fields={"title": "", "content": ""})
        mgr.update_jobs_fields_sequential(ids, fields={"hashtags": []})
        after = [mgr.get_by_id(i) or {} for i in ids]
        check(
            "xóa tiêu đề + hashtag hàng loạt",
            all(not r.get("title") and not r.get("content") and not r.get("hashtags") for r in after),
        )
        for r in after:
            validate_queue_job_payload(r)
            validate_page_for_post_job(pages[r["page_id"]], r["account_id"])
        check("job sau khi xóa tiêu đề vẫn hợp lệ để đăng", True)


def main() -> int:
    """Chạy toàn luồng và in bảng kết quả."""
    with tempfile.TemporaryDirectory(prefix="toolfb_e2e_") as tmp:
        work = Path(tmp)
        store, _jobs = step_download(work)
        items = step_editor_and_export(work, store)
        step_pending_and_schedule(items)
    print("\n===== KẾT QUẢ E2E =====")
    for name, ok, detail in RESULTS:
        print(f"[{'OK ' if ok else 'LỖI'}] {name}" + (f" — {detail}" if detail else ""))
    n_bad = sum(1 for _n, ok, _d in RESULTS if not ok)
    print(f"Tổng: {len(RESULTS) - n_bad} OK, {n_bad} lỗi")
    print(json.dumps({"ok": n_bad == 0}, ensure_ascii=False))
    return 0 if n_bad == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
