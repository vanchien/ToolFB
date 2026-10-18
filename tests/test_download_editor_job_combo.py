"""Job tải → combobox Video Editor."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from src.services.universal_video_downloader import (
    DownloadMetadataStore,
    unique_playable_download_rows,
    _merge_downloader_metadata_canonical,
    _read_json_object_list_file,
    build_download_job_combo_options,
    prune_unused_download_jobs,
    clear_pending_video_editor_job,
    discover_downloader_data_roots,
    downloader_layout_candidate_roots,
    downloader_metadata_summary,
    ensure_downloader_layout,
    list_videos_for_download_job,
    read_pending_video_editor_job,
    write_pending_video_editor_job,
)
from src.utils.paths import reset_project_root_cache


def test_unique_rows_keep_one_larger_copy(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        "src.services.universal_video_downloader.downloaded_file_empty_reason",
        lambda _p: "",
    )
    small = tmp_path / "a_tiktok_7414067670032633106.mp4"
    large = tmp_path / "b_tiktok_7414067670032633106.mp4"
    other = tmp_path / "c_tiktok_7414067670032633999.mp4"
    small.write_bytes(b"a" * 1000)
    large.write_bytes(b"b" * 4000)
    other.write_bytes(b"c" * 2000)
    rows = [
        {"video_path": str(small), "status": "downloaded", "duration": 2, "source_url": "https://www.tiktok.com/@x/video/7414067670032633106"},
        {"video_path": str(large), "status": "downloaded", "duration": 2, "source_url": "https://www.tiktok.com/@y/video/7414067670032633106"},
        {"video_path": str(other), "status": "downloaded", "duration": 2, "source_url": "https://www.tiktok.com/@z/video/7414067670032633999"},
        {"video_path": str(other), "status": "lỗi", "duration": 0},
    ]
    kept, dropped = unique_playable_download_rows(rows)
    assert dropped == 1
    assert len(kept) == 2
    assert Path(kept[0]["video_path"]).stat().st_size == 4000


def test_combo_shows_job_with_videos() -> None:
    jobs = [
        {
            "id": "dl_abc123",
            "name": "FB batch",
            "platform": "facebook",
            "status": "completed",
            "created_at": "2026-06-01T10:00:00",
        }
    ]
    videos = [
        {"id": "v1", "download_job_id": "dl_abc123", "video_path": "/a.mp4"},
        {"id": "v2", "download_job_id": "dl_abc123", "video_path": "/b.mp4"},
    ]
    vals, mapping = build_download_job_combo_options(jobs, videos, show_empty=False)
    assert len(vals) == 2
    assert mapping[vals[0]] == "__all_downloaded__"
    assert "2 video" in vals[1]
    assert mapping[vals[1]] == "dl_abc123"


def test_combo_hides_completed_job_without_videos() -> None:
    jobs = [
        {
            "id": "dl_empty1",
            "platform": "youtube",
            "status": "completed",
            "created_at": "2026-06-02T12:00:00",
        }
    ]
    vals, _mapping = build_download_job_combo_options(jobs, [], show_empty=False)
    assert vals == []
    shown, mapping = build_download_job_combo_options(jobs, [], show_empty=True)
    assert mapping[shown[0]] == "dl_empty1"


def test_combo_hides_stale_running_job_without_videos() -> None:
    jobs = [{"id": "dl_run", "platform": "tiktok", "status": "running", "created_at": "2026-06-02T12:00:00"}]
    vals, _mapping = build_download_job_combo_options(jobs, [], show_empty=False)
    assert vals == []


def test_combo_shows_fresh_running_job_without_videos() -> None:
    now = datetime.now().replace(microsecond=0).isoformat()
    jobs = [{"id": "dl_run", "platform": "tiktok", "status": "running", "started_at": now, "updated_at": now}]
    vals, mapping = build_download_job_combo_options(jobs, [], show_empty=False)
    assert len(vals) == 1
    assert mapping[vals[0]] == "dl_run"


def test_combo_hides_pending_empty_job() -> None:
    jobs = [{"id": "dl_pend", "platform": "tiktok", "status": "pending"}]
    vals, _ = build_download_job_combo_options(jobs, [], show_empty=False)
    assert vals == []


def test_combo_orphan_videos_without_job_record() -> None:
    videos = [{"id": "v1", "download_job_id": "dl_orphan", "video_path": "/x.mp4"}]
    vals, mapping = build_download_job_combo_options([], videos, show_empty=False)
    assert len(vals) == 2
    assert mapping[vals[0]] == "__all_downloaded__"
    assert mapping[vals[1]] == "dl_orphan"


def test_read_json_list_survives_transient_bad_file(tmp_path: Path) -> None:
    p = tmp_path / "download_jobs.json"
    p.write_text("[", encoding="utf-8")
    assert _read_json_object_list_file(p, retries=2) == []
    p.write_text('[{"id": "ok"}]\n', encoding="utf-8")
    rows = _read_json_object_list_file(p, retries=3)
    assert rows and rows[0]["id"] == "ok"


def test_prune_unused_download_jobs_keeps_videos(tmp_path: Path) -> None:
    paths = {
        "root": tmp_path,
        "jobs_file": tmp_path / "download_jobs.json",
        "videos_file": tmp_path / "downloaded_videos.json",
        "archive": tmp_path / "archive.txt",
    }
    store = DownloadMetadataStore(paths=paths)
    now = datetime.now().replace(microsecond=0).isoformat()
    store.save_job({"id": "dl_keep", "name": "catmovie", "platform": "tiktok", "status": "running", "updated_at": now})
    store.save_job({"id": "dl_fail", "name": "catmovie", "platform": "tiktok", "status": "failed", "created_at": "2026-06-01T10:00:00"})
    store.save_job({"id": "dl_stuck", "name": "yt", "platform": "youtube", "status": "running", "started_at": "2026-06-01T10:00:00"})
    store.save_downloaded_videos(
        [{"id": "v1", "download_job_id": "dl_keep", "video_path": str(tmp_path / "a.mp4")}]
    )
    removed = prune_unused_download_jobs(store)
    assert set(removed) == {"dl_fail", "dl_stuck"}
    left = {str(j.get("id")) for j in store.list_jobs()}
    assert left == {"dl_keep"}
    assert store.list_downloaded_videos()[0]["id"] == "v1"


def test_metadata_store_atomic_roundtrip(tmp_path: Path) -> None:
    paths = {
        "root": tmp_path,
        "jobs_file": tmp_path / "download_jobs.json",
        "videos_file": tmp_path / "downloaded_videos.json",
        "archive": tmp_path / "archive.txt",
    }
    store = DownloadMetadataStore(paths=paths)
    store.save_job({"id": "dl_x", "platform": "youtube", "status": "completed"})
    jobs = store.list_jobs()
    assert len(jobs) == 1
    assert jobs[0]["id"] == "dl_x"


def test_merge_downloader_metadata_from_exe_gui_folder(tmp_path: Path, monkeypatch) -> None:
    """Metadata trong exe_gui/data được gộp về data/downloader chuẩn khi chạy từ thư mục cha."""
    install = tmp_path / "ToolFB"
    exe_gui = install / "exe_gui"
    legacy_dl = exe_gui / "data" / "downloader"
    legacy_dl.mkdir(parents=True)
    (legacy_dl / "download_jobs.json").write_text(
        json.dumps(
            [{"id": "dl_legacy1", "platform": "instagram", "status": "completed", "created_at": "2026-06-01T10:00:00"}],
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )
    (legacy_dl / "downloaded_videos.json").write_text(
        json.dumps(
            [{"id": "v1", "download_job_id": "dl_legacy1", "video_path": str(tmp_path / "a.mp4")}],
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("TOOLFB_DATA_DIR", str(install))
    reset_project_root_cache()
    _merge_downloader_metadata_canonical()
    paths = ensure_downloader_layout()
    jobs = _read_json_object_list_file(paths["jobs_file"])
    videos = _read_json_object_list_file(paths["videos_file"])
    assert any(str(j.get("id")) == "dl_legacy1" for j in jobs)
    assert len(videos) == 1
    meta = downloader_metadata_summary()
    assert int(meta["job_count"]) >= 1
    reset_project_root_cache()
    monkeypatch.delenv("TOOLFB_DATA_DIR", raising=False)


def test_merge_downloader_metadata_from_portable_clean_sibling(tmp_path: Path, monkeypatch) -> None:
    """Chạy .exe trong exe_gui/ vẫn thấy job tải từ portable_clean/ (cùng bundle)."""
    bundle = tmp_path / "ToolFB_release_bundle"
    portable = bundle / "portable_clean"
    exe_gui = bundle / "exe_gui"
    pc_dl = portable / "data" / "downloader"
    pc_dl.mkdir(parents=True)
    (pc_dl / "download_jobs.json").write_text(
        json.dumps(
            [{"id": "dl_pc1", "platform": "youtube", "status": "completed", "created_at": "2026-06-03T10:00:00"}],
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )
    (pc_dl / "downloaded_videos.json").write_text(
        json.dumps(
            [{"id": "v_pc", "download_job_id": "dl_pc1", "video_path": str(tmp_path / "clip.mp4")}],
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )
    (pc_dl / "pending_video_editor_job.json").write_text(
        json.dumps({"job_id": "dl_pc1", "saved_at": "2026-06-03T12:00:00"}, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    exe_gui.mkdir(parents=True)
    monkeypatch.setenv("TOOLFB_DATA_DIR", str(exe_gui))
    reset_project_root_cache()
    _merge_downloader_metadata_canonical()
    paths = ensure_downloader_layout()
    jobs = _read_json_object_list_file(paths["jobs_file"])
    assert any(str(j.get("id")) == "dl_pc1" for j in jobs)
    assert read_pending_video_editor_job(paths=paths, consume=False) == "dl_pc1"
    reset_project_root_cache()
    monkeypatch.delenv("TOOLFB_DATA_DIR", raising=False)


def test_merge_prefers_newer_job_row(tmp_path: Path, monkeypatch) -> None:
    install = tmp_path / "App"
    exe_gui = install / "exe_gui"
    legacy_dl = exe_gui / "data" / "downloader"
    legacy_dl.mkdir(parents=True)
    (legacy_dl / "download_jobs.json").write_text(
        json.dumps(
            [
                {
                    "id": "dl_same",
                    "platform": "youtube",
                    "status": "completed",
                    "created_at": "2026-06-01T10:00:00",
                    "updated_at": "2026-06-01T10:00:00",
                    "name": "old",
                }
            ],
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )
    canon_dl = install / "data" / "downloader"
    canon_dl.mkdir(parents=True)
    (canon_dl / "download_jobs.json").write_text(
        json.dumps(
            [
                {
                    "id": "dl_same",
                    "platform": "youtube",
                    "status": "completed",
                    "created_at": "2026-06-01T09:00:00",
                    "updated_at": "2026-06-03T12:00:00",
                    "name": "new",
                }
            ],
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("TOOLFB_DATA_DIR", str(install))
    reset_project_root_cache()
    _merge_downloader_metadata_canonical()
    jobs = _read_json_object_list_file((install / "data" / "downloader" / "download_jobs.json"))
    assert jobs[0]["name"] == "new"
    reset_project_root_cache()
    monkeypatch.delenv("TOOLFB_DATA_DIR", raising=False)


def test_list_videos_fallback_downloaded_files(tmp_path: Path, monkeypatch) -> None:
    install = tmp_path / "App"
    dl_root = install / "data" / "downloader"
    dl_root.mkdir(parents=True)
    vid = tmp_path / "clip.mp4"
    vid.write_bytes(b"x")
    (dl_root / "download_jobs.json").write_text(
        json.dumps(
            [
                {
                    "id": "dl_fb",
                    "platform": "youtube",
                    "status": "completed",
                    "downloaded_files": [str(vid)],
                }
            ],
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )
    (dl_root / "downloaded_videos.json").write_text("[]\n", encoding="utf-8")
    monkeypatch.setenv("TOOLFB_DATA_DIR", str(install))
    reset_project_root_cache()
    rows = list_videos_for_download_job("dl_fb")
    assert len(rows) == 1
    assert Path(rows[0]["video_path"]).name == "clip.mp4"
    reset_project_root_cache()
    monkeypatch.delenv("TOOLFB_DATA_DIR", raising=False)


def test_discover_finds_dist_release_bundle_layout(tmp_path: Path, monkeypatch) -> None:
    repo = tmp_path / "ToolFB"
    bundle = repo / "dist" / "ToolFB_release_bundle" / "exe_gui"
    dl = bundle / "data" / "downloader"
    dl.mkdir(parents=True)
    (dl / "download_jobs.json").write_text(
        json.dumps([{"id": "dl_dist", "status": "completed"}], ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("TOOLFB_DATA_DIR", str(repo))
    reset_project_root_cache()
    roots = discover_downloader_data_roots()
    assert any("dl_dist" in json.dumps(_read_json_object_list_file(r / "download_jobs.json")) for r in roots)
    reset_project_root_cache()
    monkeypatch.delenv("TOOLFB_DATA_DIR", raising=False)


def test_pending_job_read_from_legacy_root(tmp_path: Path, monkeypatch) -> None:
    install = tmp_path / "bundle"
    legacy = install / "portable_clean" / "data" / "downloader"
    legacy.mkdir(parents=True)
    (legacy / "pending_video_editor_job.json").write_text(
        json.dumps({"job_id": "dl_pend", "saved_at": "2026-06-04T10:00:00"}, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    exe_gui = install / "exe_gui"
    exe_gui.mkdir(parents=True)
    monkeypatch.setenv("TOOLFB_DATA_DIR", str(exe_gui))
    reset_project_root_cache()
    assert read_pending_video_editor_job(consume=False) == "dl_pend"
    reset_project_root_cache()
    monkeypatch.delenv("TOOLFB_DATA_DIR", raising=False)


def test_pending_job_file_roundtrip(tmp_path: Path) -> None:
    paths = {
        "root": tmp_path,
        "jobs_file": tmp_path / "download_jobs.json",
        "videos_file": tmp_path / "downloaded_videos.json",
        "archive": tmp_path / "archive.txt",
    }
    write_pending_video_editor_job("dl_pending99", paths=paths)
    assert read_pending_video_editor_job(paths=paths, consume=False) == "dl_pending99"
    assert read_pending_video_editor_job(paths=paths, consume=True) == "dl_pending99"
    assert read_pending_video_editor_job(paths=paths, consume=False) == ""
    clear_pending_video_editor_job(paths=paths)
