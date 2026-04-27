"""
Module Destination Manager — Page/Group đích, lưu riêng ``config/pages.json``.

``PageDestinationRepository``: CRUD chỉ file pages — **không** import ``account_manager``.
Danh sách owner (account id) do caller truyền vào UI (loose coupling).
"""

from __future__ import annotations

from pathlib import Path
from typing import Union

import streamlit as st
from loguru import logger

from src.utils.pages_manager import PageRecord, PagesManager


class PageDestinationRepository:
    """
    CRUD / truy vấn chỉ cho ``config/pages.json``.

    Không phụ thuộc ``account_manager`` — không đọc ``accounts.json``.
    """

    def __init__(self, backend: PagesManager | None = None, *, json_path: str | Path | None = None) -> None:
        if backend is not None:
            self._db = backend
        elif json_path is not None:
            self._db = PagesManager(json_path)
        else:
            self._db = PagesManager()

    @property
    def backend(self) -> PagesManager:
        return self._db

    @property
    def config_path(self) -> Path:
        return self._db.file_path

    def load_all(self) -> list[PageRecord]:
        return self._db.load_all()

    def get_by_id(self, page_id: str) -> PageRecord | None:
        return self._db.get_by_id(page_id)

    def upsert(self, row: PageRecord) -> None:
        self._db.upsert(row)

    def delete_by_id(self, page_id: str) -> bool:
        return self._db.delete_by_id(page_id)

    def list_for_account(self, account_id: str) -> list[PageRecord]:
        return self._db.list_for_account(account_id)

    def reload_from_disk(self) -> list[PageRecord]:
        return self._db.reload_from_disk()


PageStoreLike = Union[PagesManager, PageDestinationRepository]


def _pages_backend(store: PageStoreLike) -> PagesManager:
    return store.backend if isinstance(store, PageDestinationRepository) else store


def render_destination_manager_page(
    pages: PageStoreLike,
    owner_account_ids: list[str],
) -> None:
    """
    Trang Streamlit: quản lý Page/Group (pages.json).

    Args:
        pages: ``PageDestinationRepository`` hoặc ``PagesManager``.
        owner_account_ids: Danh sách ``id`` tài khoản (do dashboard đọc từ accounts.json).
    """
    pmgr = _pages_backend(pages)
    st.header("Quản lý Page / Group (Destination Manager)")
    st.caption(
        "Mỗi Page: phân loại (Fanpage / Profile / Group), URL, phong cách đăng (post / image / video), "
        f"owner = id tài khoản. File: `{pmgr.file_path}` — tách khỏi accounts.json."
    )

    ids = [str(x).strip() for x in owner_account_ids if str(x).strip()]
    if not ids:
        st.warning("Chưa có tài khoản nào — thêm Account Source trước.")
        return

    st.subheader("Danh sách Page / Group")
    try:
        page_rows = pmgr.load_all()
    except Exception as exc:  # noqa: BLE001
        st.error(str(exc))
        return
    if page_rows:
        st.dataframe(
            [
                {
                    "id": p.get("id"),
                    "account_id": p.get("account_id"),
                    "page_kind": p.get("page_kind", "—"),
                    "page_name": p.get("page_name"),
                    "post_style": p.get("post_style"),
                    "schedule_time": p.get("schedule_time", "—"),
                    "page_url": (str(p.get("page_url", ""))[:56] + "…")
                    if len(str(p.get("page_url", ""))) > 56
                    else p.get("page_url"),
                }
                for p in page_rows
            ],
            use_container_width=True,
        )
    else:
        st.info("Chưa có Page/Group — thêm bên dưới.")

    st.subheader("Thêm / cập nhật Page hoặc Group")
    with st.form("form_page"):
        pid = st.text_input("Page id (để trống = tạo mới)", value="")
        owner = st.selectbox("Owner (account id)", ids)
        pkind = st.selectbox(
            "Phân loại (page_kind)",
            ["", "fanpage", "profile", "group"],
            format_func=lambda x: {
                "": "— (chưa chọn)",
                "fanpage": "Fanpage",
                "profile": "Profile cá nhân",
                "group": "Group",
            }[x],
        )
        pname = st.text_input("Page_Name", value="")
        purl = st.text_input("Page_URL (https://…)", value="https://www.facebook.com/")
        pstyle = st.selectbox(
            "Post_Style",
            ["post", "image", "video"],
            format_func=lambda x: {
                "post": "Bài viết (text)",
                "image": "Hình ảnh",
                "video": "Video",
            }[x],
        )
        psch = st.text_input("Lịch gợi ý (HH:MM, tùy chọn — dùng khi nối scheduler)", value="")
        if st.form_submit_button("Lưu Page/Group"):
            row: PageRecord = {
                "id": pid.strip(),
                "account_id": owner,
                "page_name": pname.strip(),
                "page_url": purl.strip(),
                "post_style": pstyle,
            }
            if pkind.strip():
                row["page_kind"] = pkind.strip().lower()
            if psch.strip():
                row["schedule_time"] = psch.strip()
            try:
                pmgr.upsert(row)
                pmgr.reload_from_disk()
                st.success("Đã lưu pages.json.")
                st.rerun()
            except Exception as exc:  # noqa: BLE001
                st.error(str(exc))

    del_id = st.text_input("Xóa theo Page id", value="", key="page_delete_id")
    if st.button("Xóa Page/Group", key="page_delete_btn") and del_id.strip():
        if pmgr.delete_by_id(del_id.strip()):
            logger.info("page_manager: đã xóa page id={}", del_id.strip())
            st.success("Đã xóa.")
            st.rerun()
        else:
            st.warning("Không tìm thấy id.")
