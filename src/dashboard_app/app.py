"""
Dashboard web (Streamlit): 4 trang — Account, Entity, Content Studio, Điều phối.

Chạy từ thư mục gốc dự án: ``streamlit run dashboard.py``
"""

from __future__ import annotations

import os
import threading
from datetime import datetime, timedelta, timezone
import streamlit as st
from loguru import logger

from src.modules.account_manager import AccountConfigRepository, render_account_source_page
from src.modules.page_manager import PageDestinationRepository, render_destination_manager_page
from src.scheduler import run_scheduled_post_for_account
from src.utils.app_secrets import (
    apply_saved_gemini_key_to_environ,
    apply_saved_openai_key_to_environ,
    apply_saved_nanobanana_config_to_environ,
    apply_saved_nanobanana_key_to_environ,
)
from src.utils.db_manager import AccountsDatabaseManager
from src.utils.dispatch_queue import DispatchQueueStore
from src.utils.drafts_store import delete_draft, list_drafts, load_draft, save_draft
from src.utils.entities_manager import EntitiesManager
from src.utils.media_library import list_media_files, random_rename_file, save_upload_to_library
from src.utils.pages_manager import PagesManager
from src.utils.paths import project_root


def _ensure_file_logger() -> None:
    """
    Ghi log dashboard vào ``logs/dashboard_streamlit.log`` (một lần mỗi process).
    """
    if st.session_state.get("_dash_log"):
        return
    log_path = project_root() / "logs" / "dashboard_streamlit.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger.add(
        str(log_path),
        rotation="5 MB",
        retention=3,
        enqueue=True,
        level="INFO",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    )
    st.session_state["_dash_log"] = True


def _managers() -> tuple[AccountsDatabaseManager, EntitiesManager, DispatchQueueStore, PagesManager]:
    """
    Khởi tạo singleton trong ``st.session_state``.
    """
    if "accounts_mgr" not in st.session_state:
        mgr = AccountsDatabaseManager()
        st.session_state.accounts_mgr = mgr
        try:
            from src.utils.profile_cleanup import cleanup_orphan_profile_directories

            cleanup_orphan_profile_directories(mgr.load_all())
        except Exception as exc:  # noqa: BLE001
            logger.warning("Dọn profile mồ côi (dashboard): {}", exc)
    if "entities_mgr" not in st.session_state:
        st.session_state.entities_mgr = EntitiesManager()
    if "queue_mgr" not in st.session_state:
        st.session_state.queue_mgr = DispatchQueueStore()
    if "pages_mgr" not in st.session_state:
        st.session_state.pages_mgr = PagesManager()
    return (
        st.session_state.accounts_mgr,
        st.session_state.entities_mgr,
        st.session_state.queue_mgr,
        st.session_state.pages_mgr,
    )


def _page_accounts() -> None:
    """
    Trang 1: module Account Source (accounts.json) — tách khỏi Page/Group.
    """
    mgr, _, _, _ = _managers()
    render_account_source_page(AccountConfigRepository(mgr))


def _page_entities() -> None:
    """
    Trang 2: Destination Manager — ``config/pages.json`` (tách khỏi accounts).
    """
    mgr, _, _, pmgr = _managers()
    owner_ids = [str(a.get("id", "")) for a in mgr.load_all() if a.get("id")]
    render_destination_manager_page(PageDestinationRepository(pmgr), owner_ids)


def _page_content() -> None:
    """
    Trang 3: AI Gemini, bản thảo, media, preview.
    """
    st.header("Content Studio")
    st.caption("Nội dung unique — Gemini; lưu draft; media đổi tên ngẫu nhiên; preview trước khi xếp hàng.")
    tab1, tab2, tab3 = st.tabs(["Sinh bài & Draft", "Kho media", "Preview draft"])

    with tab1:
        topic = st.text_input("Chủ đề / từ khóa", value="")
        style = st.text_input("Phong cách (tùy chọn)", value=os.environ.get("CONTENT_STYLE", ""))
        if st.button("Gọi Gemini tạo bài"):
            if not os.environ.get("GEMINI_API_KEY", "").strip():
                st.error("Thiếu GEMINI_API_KEY trong môi trường.")
            else:
                try:
                    from src.ai.gemini_api import generate_post

                    post = generate_post(topic, style=style or None)
                    st.session_state["_last_gen"] = post
                    st.json(post)
                except Exception as exc:  # noqa: BLE001
                    st.error(str(exc))
        post = st.session_state.get("_last_gen")
        if isinstance(post, dict) and post.get("body"):
            if st.button("Lưu thành bản thảo (draft)"):
                rec = save_draft(
                    topic=topic or post.get("body", "")[:80],
                    body=str(post.get("body", "")),
                    image_alt=str(post.get("image_alt", "")),
                )
                st.success(f"Đã lưu draft id={rec['id']}")

        st.subheader("Kho bản thảo")
        drafts = list_drafts()
        if drafts:
            opts = {f"{d.get('id')} — {d.get('topic', '')[:40]}": str(d.get("id")) for d in drafts}
            pick = st.selectbox("Chọn draft", list(opts.keys()), key="draft_del_pick")
            if st.button("Xóa draft đã chọn"):
                delete_draft(opts[pick])
                st.rerun()
        else:
            st.info("Chưa có draft.")

    with tab2:
        st.subheader("Upload ảnh/video")
        f = st.file_uploader("Chọn file", type=["jpg", "jpeg", "png", "webp", "gif", "mp4", "webm"])
        if f and st.button("Lưu vào thư viện media"):
            path = save_upload_to_library(f.getvalue(), f.name)
            st.success(str(path.relative_to(project_root())))

        files = list_media_files()
        if files:
            sel = st.selectbox("File trong thư viện", [p.name for p in files], key="media_lib_pick")
            if st.button("Đổi tên ngẫu nhiên (chống trùng hash)"):
                p = next(x for x in files if x.name == sel)
                newp = random_rename_file(p)
                st.success(f"→ {newp.name}")
                st.rerun()
        else:
            st.caption("Thư mục data/media_library trống.")

    with tab3:
        drafts = list_drafts()
        if not drafts:
            st.info("Chưa có draft để preview.")
        else:
            opts = {f"{d.get('id')} — {d.get('topic', '')}": str(d.get("id")) for d in drafts}
            pick = st.selectbox("Draft", list(opts.keys()), key="draft_preview_pick")
            d = load_draft(opts[pick])
            if d:
                st.markdown("### Nội dung")
                st.markdown(str(d.get("body", "")))
                st.caption(f"Alt ảnh: {d.get('image_alt', '')}")


def _page_scheduler() -> None:
    """
    Trang 4: lịch tổng quan, hàng đợi, log, chạy khẩn cấp.
    """
    st.header("Điều phối & Hẹn giờ")
    mgr, emgr, qmgr, _pmgr = _managers()

    st.subheader("Lịch 7 ngày tới (tóm tắt)")
    today = datetime.now(timezone.utc).date()
    rows_out: list[dict[str, str]] = []
    for d in range(7):
        day = today + timedelta(days=d)
        rows_out.append(
            {
                "ngày": str(day),
                "ghi chú": "Cron hàng ngày theo schedule_time từng Page (fallback: tài khoản nếu chưa có Page có lịch)",
            }
        )
    st.dataframe(rows_out, use_container_width=True)

    st.subheader("Lịch từ Page / tài khoản & entity")
    try:
        pm = _pmgr.load_all()
        page_rows = [
            {"nguồn": f"page:{p.get('id')}", "giờ": p.get("schedule_time"), "mô tả": p.get("page_name")}
            for p in pm
            if str(p.get("schedule_time", "")).strip()
        ]
        acc_rows = [
            {"nguồn": f"account:{a.get('id')}", "giờ": a.get("schedule_time"), "mô tả": a.get("name")}
            for a in mgr.load_all()
        ]
        ent_rows = [{"nguồn": f"entity:{e.get('id')}", "giờ": e.get("schedule_time"), "mô tả": e.get("name")} for e in emgr.load_all()]
        st.dataframe(page_rows + acc_rows + ent_rows, use_container_width=True)
    except Exception as exc:  # noqa: BLE001
        st.error(str(exc))

    st.subheader("Hàng đợi (Queue)")
    jobs = qmgr.load_all()
    if jobs:
        st.dataframe(jobs[::-1][:100], use_container_width=True)
    else:
        st.caption("Chưa có job — dùng «Chạy khẩn cấp» hoặc automation sau này.")

    st.subheader("Log dashboard (tail)")
    logf = project_root() / "logs" / "dashboard_streamlit.log"
    if logf.is_file():
        lines = logf.read_text(encoding="utf-8", errors="replace").splitlines()[-200:]
        st.code("\n".join(lines), language=None)
    else:
        st.caption("Chưa có file log — thao tác trên dashboard để tạo log.")

    st.subheader("Chạy khẩn cấp (bỏ qua lịch cron)")
    st.warning(
        "Sẽ chạy pipeline đăng (draft hoặc AI + Playwright, tùy entity) trong thread nền — không chặn giao diện."
    )
    ids = [str(a.get("id", "")) for a in mgr.load_all() if a.get("id")]
    if ids:
        run_id = st.selectbox("Tài khoản chạy ngay", ids, key="emergency_acc")
        ent_choices: dict[str, str] = {"— Timeline (không entity) —": ""}
        for ent in emgr.list_for_account(run_id):
            eid = str(ent.get("id", "")).strip()
            if eid:
                ent_choices[f"{ent.get('name', eid)} ({eid})"] = eid
        ent_label = st.selectbox("Đích đăng (entity)", list(ent_choices.keys()), key="emergency_entity")
        sel_entity_id = ent_choices[ent_label]

        draft_choices: dict[str, str] = {"— AI tự sinh (không draft) —": ""}
        for dr in list_drafts():
            did = str(dr.get("id", "")).strip()
            if did:
                draft_choices[f"{did} — {dr.get('topic', '')}"] = did
        draft_label = st.selectbox("Nội dung (draft)", list(draft_choices.keys()), key="emergency_draft")
        sel_draft_id = draft_choices[draft_label]

        if st.button("Chạy khẩn cấp", type="primary"):
            job = qmgr.append_job(
                account_id=run_id,
                entity_id=sel_entity_id,
                draft_id=sel_draft_id,
                status="pending",
            )
            jid = str(job["id"])
            mgr_ref = mgr
            q_ref = qmgr

            def _job(
                job_id: str = jid,
                account_id: str = run_id,
                entity_id_run: str = sel_entity_id,
                draft_id_run: str = sel_draft_id,
                accounts_mgr: AccountsDatabaseManager = mgr_ref,
                qstore: DispatchQueueStore = q_ref,
            ) -> None:
                qstore.update_job(job_id, status="processing")
                try:
                    run_scheduled_post_for_account(
                        account_id,
                        entity_id=entity_id_run or None,
                        draft_id=draft_id_run or None,
                        accounts=accounts_mgr,
                    )
                    qstore.update_job(job_id, status="done")
                except Exception as exc:  # noqa: BLE001
                    logger.exception("Emergency run: {}", exc)
                    qstore.update_job(job_id, status="failed", error_message=str(exc)[:500])

            threading.Thread(target=_job, name="emergency_post", daemon=True).start()
            st.success("Đã gửi job nền — xem log Playwright / failed_accounts.log.")
    else:
        st.info("Không có tài khoản.")


def run_dashboard() -> None:
    """
    Điểm vào Streamlit: cấu hình trang + sidebar + điều hướng 4 mục.
    """
    apply_saved_gemini_key_to_environ()
    apply_saved_openai_key_to_environ()
    apply_saved_nanobanana_key_to_environ()
    apply_saved_nanobanana_config_to_environ()
    _ensure_file_logger()
    st.set_page_config(page_title="ToolFB Dashboard", layout="wide", initial_sidebar_state="expanded")
    st.sidebar.title("ToolFB")
    st.sidebar.markdown("**Luồng:** Account Source → Page/Group (pages.json) → Content → Điều phối")
    page = st.sidebar.radio(
        "Trang",
        [
            "1. Account Source",
            "2. Page / Group (pages.json)",
            "3. Content Studio",
            "4. Điều phối & Hẹn giờ",
        ],
    )
    _managers()
    if page.startswith("1"):
        _page_accounts()
    elif page.startswith("2"):
        _page_entities()
    elif page.startswith("3"):
        _page_content()
    else:
        _page_scheduler()
