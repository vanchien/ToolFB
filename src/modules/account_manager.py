"""
Module Account Source — chỉ danh tính tài khoản (không trộn Page/Group).

Giao diện Streamlit: thêm/sửa account, import cookie (file hoặc dán chuỗi),
Verify Profile (Playwright), kiểm tra proxy. Lưu ``config/accounts.json``.
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any, Union

import streamlit as st
from loguru import logger

from src.modules.browser_engine import BrowserEngine
from src.utils.db_manager import AccountRecord, AccountsDatabaseManager
from src.utils.paths import project_root
from src.utils.proxy_check import check_http_proxy


class AccountConfigRepository:
    """
    CRUD / truy vấn chỉ cho ``config/accounts.json`` (danh tính + proxy + path).

    Không phụ thuộc ``page_manager`` — chỉ dùng ``AccountsDatabaseManager`` nội bộ.
    """

    def __init__(
        self,
        backend: AccountsDatabaseManager | None = None,
        *,
        json_path: str | Path | None = None,
    ) -> None:
        if backend is not None:
            self._db = backend
        elif json_path is not None:
            self._db = AccountsDatabaseManager(json_path)
        else:
            self._db = AccountsDatabaseManager()

    @property
    def backend(self) -> AccountsDatabaseManager:
        return self._db

    @property
    def config_path(self) -> Path:
        return self._db.file_path

    def load_all(self) -> list[AccountRecord]:
        return self._db.load_all()

    def get_by_id(self, account_id: str) -> AccountRecord | None:
        return self._db.get_by_id(account_id)

    def validate_account(self, record: dict[str, Any]) -> None:
        self._db.validate_account(record)

    def upsert(self, account: AccountRecord) -> None:
        self._db.upsert(account)

    def update_account_fields(self, account_id: str, updates: dict[str, Any]) -> None:
        self._db.update_account_fields(account_id, updates)

    def delete_by_id(self, account_id: str) -> bool:
        return self._db.delete_by_id(account_id)

    def reload_from_disk(self) -> list[AccountRecord]:
        return self._db.reload_from_disk()


AccountStoreLike = Union[AccountsDatabaseManager, AccountConfigRepository]


def _account_db(store: AccountStoreLike) -> AccountsDatabaseManager:
    return store.backend if isinstance(store, AccountConfigRepository) else store


def cookie_file_relative(account_id: str) -> str:
    """Đường dẫn tương đối gốc dự án: ``data/cookies/{account_id}.json``."""
    safe = str(account_id).strip().replace("/", "").replace("\\", "")
    return f"data/cookies/{safe}.json"


def normalize_cookie_json_payload(raw: Any) -> list[Any]:
    """
    Chuẩn hóa upload/dán thành mảng cookie Playwright.

    Raises:
        ValueError: Không parse được hoặc sai cấu trúc.
    """
    if isinstance(raw, str):
        data = json.loads(raw)
    else:
        data = raw
    if isinstance(data, dict) and "cookies" in data:
        data = data["cookies"]
    if not isinstance(data, list):
        raise ValueError("Cookie phải là mảng JSON hoặc object có khóa «cookies».")
    return data


def save_cookies_to_data_dir(account_id: str, payload: Any) -> Path:
    """
    Ghi cookie vào ``data/cookies/{account_id}.json`` (tuyệt đối trả về Path).

    Raises:
        ValueError: Payload không hợp lệ.
    """
    cookies = normalize_cookie_json_payload(payload)
    rel = cookie_file_relative(account_id)
    dest = project_root() / rel
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(cookies, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    logger.info("Đã lưu cookie cho account_id={} → {}", account_id, dest)
    return dest


def verify_portable_profile(
    store: AccountStoreLike,
    account_id: str,
    *,
    headless: bool = True,
) -> tuple[bool, str]:
    """
    Mở thử persistent context theo cấu hình tài khoản (kiểm tra Portable Path + proxy).

    Returns:
        (True, message) nếu mở được và tải ``about:blank``; (False, lỗi) nếu thất bại.
    """
    return BrowserEngine.verify_profile_ready(_account_db(store), account_id, headless=headless)


def render_account_source_page(store: AccountStoreLike) -> None:
    """
    Trang Streamlit: Account Source — nuôi danh tính tài khoản (accounts.json).

    Args:
        store: ``AccountConfigRepository`` hoặc ``AccountsDatabaseManager`` (tương thích dashboard).
    """
    mgr = _account_db(store)
    st.header("Quản lý Account (Account Source)")
    st.caption(
        f"Chỉ cookie + profile portable + proxy + lịch mặc định — không gộp Page/Group. "
        f"File: `{mgr.file_path}`"
    )

    st.subheader("Thêm tài khoản mới")
    with st.form("form_new_account"):
        n_name = st.text_input("Name *", value="")
        n_browser = st.selectbox("Browser Type *", ["firefox", "chromium", "webkit"], index=0)
        n_portable = st.text_input(
            "Portable Path * (tương đối, ví dụ data/profiles/my_acc)",
            value="",
        )
        st.markdown("**Proxy**")
        c1, c2, c3, c4 = st.columns(4)
        n_ph = c1.text_input("Host", value="", key="new_px_host")
        n_pp = c2.text_input("Port", value="0", key="new_px_port")
        n_pu = c3.text_input("User", value="", key="new_px_user")
        n_ppw = c4.text_input("Pass", value="", type="password", key="new_px_pass")
        n_schedule = st.text_input("Giờ đăng mặc định (HH:MM)", value="09:00")
        n_topic = st.text_input("Topic AI (tùy chọn)", value="")
        n_style = st.text_input("Content style (tùy chọn)", value="")
        if st.form_submit_button("Tạo tài khoản"):
            aid = f"acc_{uuid.uuid4().hex[:12]}"
            if not n_name.strip():
                st.error("Name không được để trống.")
                return
            if not n_portable.strip():
                st.error("Portable Path không được để trống.")
                return
            try:
                port_int = int(str(n_pp).strip() or "0")
            except ValueError:
                st.error("Port proxy phải là số nguyên.")
                return
            rec: dict[str, Any] = {
                "id": aid,
                "name": n_name.strip(),
                "browser_type": n_browser,
                "portable_path": n_portable.strip(),
                "proxy": {
                    "host": n_ph.strip(),
                    "port": port_int,
                    "user": n_pu.strip(),
                    "pass": n_ppw.strip(),
                },
                "cookie_path": cookie_file_relative(aid),
                "schedule_time": n_schedule.strip() or "09:00",
                "status": "pending",
            }
            if n_topic.strip():
                rec["topic"] = n_topic.strip()
            if n_style.strip():
                rec["content_style"] = n_style.strip()
            try:
                mgr.validate_account(rec)  # type: ignore[arg-type]
                mgr.upsert(rec)  # type: ignore[arg-type]
                (project_root() / rec["cookie_path"]).parent.mkdir(parents=True, exist_ok=True)
                if not (project_root() / rec["cookie_path"]).is_file():
                    (project_root() / rec["cookie_path"]).write_text("[]\n", encoding="utf-8")
                mgr.reload_from_disk()
                st.success(f"Đã tạo tài khoản id={aid} — cookie rỗng [] tại {rec['cookie_path']}")
                st.rerun()
            except Exception as exc:  # noqa: BLE001
                st.error(str(exc))

    st.divider()
    ids = [str(a.get("id", "")) for a in mgr.load_all() if a.get("id")]
    if not ids:
        st.warning("Chưa có tài khoản trong accounts.json.")
        return

    st.subheader("Chỉnh sửa tài khoản đã có")
    aid = st.selectbox("Chọn tài khoản", ids, key="acct_src_pick")
    got = mgr.get_by_id(aid)
    acc = dict(got) if got else {}

    with st.form("form_edit_account"):
        c1, c2 = st.columns(2)
        name = c1.text_input("Tên hiển thị", value=str(acc.get("name", "")))
        _browser_order = ["firefox", "chromium", "webkit"]
        _cur_bt = str(acc.get("browser_type", "firefox") or "firefox").strip().lower()
        if _cur_bt == "chrome":
            _cur_bt = "chromium"
        _browser_idx = _browser_order.index(_cur_bt) if _cur_bt in _browser_order else 0
        browser = c2.selectbox(
            "Trình duyệt",
            _browser_order,
            index=_browser_idx,
        )
        portable = st.text_input("Profile portable (đường dẫn tương đối)", value=str(acc.get("portable_path", "")))
        cookie_path = st.text_input("Đường dẫn file cookie JSON", value=str(acc.get("cookie_path", "")))
        schedule = st.text_input("Giờ đăng mặc định (HH:MM)", value=str(acc.get("schedule_time", "09:00")))
        st.markdown("**Proxy**")
        px = acc.get("proxy") or {}
        pc1, pc2, pc3, pc4 = st.columns(4)
        ph = pc1.text_input("Host", value=str(px.get("host", "")))
        pp = pc2.text_input("Port", value=str(px.get("port", "")))
        pu = pc3.text_input("User", value=str(px.get("user", "")))
        ppw = pc4.text_input("Pass", value=str(px.get("pass", "")), type="password")
        topic = st.text_input("Chủ đề AI mặc định (topic)", value=str(acc.get("topic", "")))
        style = st.text_input("Phong cách AI (content_style)", value=str(acc.get("content_style", "")))
        img_path = st.text_input("post_image_path (tùy chọn)", value=str(acc.get("post_image_path", "")))
        login_opts = ("unknown", "active", "cookie_invalid")
        ls = str(acc.get("login_status", "unknown"))
        ls_i = login_opts.index(ls) if ls in login_opts else 0
        login_status = st.selectbox(
            "Trạng thái đăng nhập",
            login_opts,
            index=ls_i,
            format_func=lambda x: {
                "unknown": "Chưa xác minh",
                "active": "Cookie còn hiệu lực",
                "cookie_invalid": "Cần nạp lại cookie",
            }[x],
        )
        submitted = st.form_submit_button("Lưu thay đổi tài khoản")
        if submitted:
            try:
                port_int = int(str(pp).strip() or "0")
            except ValueError:
                st.error("Port proxy phải là số nguyên.")
                return
            merged: dict[str, Any] = {
                **acc,
                "name": name.strip(),
                "browser_type": browser,
                "portable_path": portable.strip(),
                "cookie_path": cookie_path.strip(),
                "schedule_time": schedule.strip(),
                "proxy": {"host": ph.strip(), "port": port_int, "user": pu.strip(), "pass": ppw.strip()},
                "login_status": login_status,
            }
            if topic.strip():
                merged["topic"] = topic.strip()
            else:
                merged.pop("topic", None)
            if style.strip():
                merged["content_style"] = style.strip()
            else:
                merged.pop("content_style", None)
            if img_path.strip():
                merged["post_image_path"] = img_path.strip()
            else:
                merged.pop("post_image_path", None)
            try:
                mgr.validate_account(merged)
                mgr.upsert(merged)  # type: ignore[arg-type]
                mgr.reload_from_disk()
                st.success("Đã lưu tài khoản.")
                logger.info("account_manager: cập nhật account {}", aid)
            except Exception as exc:  # noqa: BLE001
                st.error(str(exc))

    st.divider()
    st.subheader("Import Cookies")
    st.caption("Chỉ file .json hoặc dán chuỗi JSON — lưu vào data/cookies/{account_id}.json và cập nhật cookie_path.")
    up = st.file_uploader("Upload file .json", type=["json"], key="acct_cookie_upload")
    pasted = st.text_area("Hoặc dán JSON (mảng cookie hoặc {cookies: [...]})", height=160, key="acct_cookie_paste")
    col_a, col_b = st.columns(2)
    with col_a:
        if st.button("Lưu cookie từ file", key="btn_cookie_file") and up:
            try:
                raw = json.loads(up.getvalue().decode("utf-8"))
                save_cookies_to_data_dir(aid, raw)
                rel = cookie_file_relative(aid)
                mgr.update_account_fields(aid, {"cookie_path": rel})
                mgr.reload_from_disk()
                st.success(f"Đã lưu → {rel}")
            except Exception as exc:  # noqa: BLE001
                st.error(str(exc))
    with col_b:
        if st.button("Lưu cookie từ vùng dán", key="btn_cookie_paste") and pasted.strip():
            try:
                save_cookies_to_data_dir(aid, pasted.strip())
                rel = cookie_file_relative(aid)
                mgr.update_account_fields(aid, {"cookie_path": rel})
                mgr.reload_from_disk()
                st.success(f"Đã lưu → {rel}")
            except Exception as exc:  # noqa: BLE001
                st.error(str(exc))

    st.divider()
    st.subheader("Verify Profile")
    st.caption("Mở thử Chromium + profile portable + proxy (headless) và tải about:blank.")
    if st.button("Verify Profile", type="primary", key="btn_verify_profile"):
        with st.spinner("Đang mở trình duyệt…"):
            ok, msg = verify_portable_profile(store, aid, headless=True)
        if ok:
            st.success(msg)
        else:
            st.error(msg)

    st.divider()
    st.subheader("Kiểm tra Proxy (Live / Die)")
    if st.button("Kiểm tra proxy của tài khoản đang chọn", key="btn_proxy_check"):
        px = acc.get("proxy") or {}
        try:
            port_int = int(str(px.get("port", 0)))
        except (TypeError, ValueError):
            st.error("Port không hợp lệ.")
            return
        ok, msg = check_http_proxy(
            str(px.get("host", "")),
            port_int,
            user=str(px.get("user", "")),
            password=str(px.get("pass", "")),
        )
        if ok:
            st.success(f"Proxy LIVE — IP: {msg}")
        else:
            st.error(f"Proxy DIE / lỗi: {msg}")

    st.divider()
    st.subheader("Xóa tài khoản")
    st.caption("Xóa bản ghi khỏi accounts.json (không xóa file profile/cookie trên đĩa).")
    if st.button("Xóa tài khoản đang chọn", type="secondary", key="btn_delete_account"):
        if mgr.delete_by_id(aid):
            mgr.reload_from_disk()
            st.success("Đã xóa.")
            st.rerun()
        else:
            st.warning("Không xóa được (kiểm tra id).")
