"""
Ghép danh sách tài khoản với proxy theo dòng (dòng i ↔ dòng i).

Ràng buộc: số proxy ≥ số luồng đồng thời; **mỗi IP:port chỉ một tài khoản**
(đăng nhập hay chưa — không chia sẻ proxy giữa hai UID).
"""

from __future__ import annotations

import re
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, TypedDict
from urllib.parse import urlparse

from loguru import logger

from src.models.mapped_account import MappedAccount, MappedAccountAuth, MappedAccountNetwork, MappedAccountStorage
from src.utils.account_browser_profile import default_cookie_path, default_portable_path, normalize_browser_storage
from src.utils.proxy_check import (
    apply_proxy_scheme_to_config,
    check_proxy,
    check_proxy_line,
    format_proxy_line,
    format_proxy_server_url,
    parse_proxy_line,
    playwright_host_for_scheme,
)


class AccountProxyMappingError(ValueError):
    """Lỗi ghép account/proxy — dừng toàn bộ chu kỳ."""


def _non_empty_lines(text: str) -> list[str]:
    return [ln.strip() for ln in str(text or "").splitlines() if ln.strip() and not ln.strip().startswith("#")]


def read_lines_file(path: str | Path) -> list[str]:
    """Đọc file text, bỏ dòng trống và comment ``#``."""
    p = Path(path)
    if not p.is_file():
        raise FileNotFoundError(f"Không tìm thấy file: {p}")
    return _non_empty_lines(p.read_text(encoding="utf-8-sig"))


def split_account_fields(line: str) -> list[str]:
    """
    Tách một dòng tài khoản thành tối đa 6 trường.

    Định dạng chuẩn (theo thứ tự):
    ``uid|pass|2fa|mail|pass_mail|mail_khoi_phuc``

    Hỗ trợ phân tách: ``|`` (ưu tiên), tab (Excel), ``;``.
    """
    raw = str(line or "").strip()
    if not raw:
        raise ValueError("Dòng tài khoản rỗng.")
    if "|" in raw:
        parts = [p.strip() for p in raw.split("|")]
    elif "\t" in raw:
        parts = [p.strip() for p in raw.split("\t")]
    elif ";" in raw:
        parts = [p.strip() for p in raw.split(";")]
    else:
        parts = [raw]
    while len(parts) < 6:
        parts.append("")
    if len(parts) > 6:
        # Giữ 6 trường đầu; phần thừa gộp vào recovery (hiếm khi dòng có | thừa).
        extra = "|".join(p for p in parts[6:] if p)
        parts = parts[:6]
        if extra:
            parts[5] = f"{parts[5]}|{extra}".strip("|") if parts[5] else extra
    return parts[:6]


def parse_account_line(line: str, *, default_browser: str = "firefox") -> MappedAccountAuth:
    """
    Parse một dòng tài khoản → ``MappedAccountAuth``.

    Thứ tự trường: uid, pass, 2fa, mail, pass_mail, mail_khoi_phuc.
    """
    _ = default_browser
    username, password, totp, email, email_pass, recovery = split_account_fields(line)
    if not username:
        raise ValueError(f"Thiếu UID/username: {line!r}")

    return MappedAccountAuth(
        username=username,
        password=password,
        two_fa_secret=totp,
        email=email,
        email_password=email_pass,
        recovery_email=recovery,
    )


def proxy_dict_to_network(px: dict[str, Any], *, scheme: str | None = None) -> MappedAccountNetwork:
    """Chuyển dict proxy ToolFB → ``MappedAccountNetwork``."""
    host = str(px.get("host") or "").strip()
    port = int(px.get("port") or 0)
    user = str(px.get("user") or "").strip()
    password = str(px.get("pass") or "").strip()
    sch = scheme or str(px.get("scheme_hint") or "")
    server = format_proxy_server_url(px, sch if sch else None) if host and port > 0 else ""
    return MappedAccountNetwork(
        proxy_server=server,
        proxy_username=user,
        proxy_password=password,
    )


def parse_proxy_line_to_network(line: str) -> MappedAccountNetwork:
    """Parse một dòng proxy file → network block."""
    px = parse_proxy_line(line)
    return proxy_dict_to_network(px)


def network_to_proxy_config(net: MappedAccountNetwork) -> dict[str, Any]:
    """Chuyển network → ``proxy`` trong accounts.json / Playwright."""
    server = str(net.proxy_server or "").strip()
    if not server:
        return {"host": "", "port": 0, "user": "", "pass": ""}
    parsed = urlparse(server if "://" in server else f"http://{server}")
    host_raw = parsed.hostname or ""
    port = int(parsed.port or 0)
    scheme = (parsed.scheme or "http").lower()
    if scheme not in ("http", "https", "socks4", "socks5"):
        scheme = "http"
    host = playwright_host_for_scheme(host_raw, scheme)  # type: ignore[arg-type]
    return {
        "host": host,
        "port": port,
        "user": net.proxy_username,
        "pass": net.proxy_password,
        "scheme_hint": scheme,
    }


def proxy_identity_key_for_network(net: MappedAccountNetwork) -> str:
    """
    Khóa duy nhất theo **IP:port** (bỏ qua user/pass trong URL).

    Hai dòng ``1.2.3.4:8080:userA:pass`` và ``1.2.3.4:8080:userB:pass`` → cùng một khóa.
    """
    px = network_to_proxy_config(net)
    host_field = str(px.get("host") or "").strip()
    port = int(px.get("port") or 0)
    host = ""
    if host_field:
        if "://" in host_field:
            parsed = urlparse(host_field)
            host = (parsed.hostname or "").strip().lower()
            if not port and parsed.port:
                port = int(parsed.port)
        else:
            host = host_field.split("@")[-1].strip().lower()
    if not host:
        server = str(net.proxy_server or "").strip()
        if server:
            parsed = urlparse(server if "://" in server else f"http://{server}")
            host = (parsed.hostname or "").strip().lower()
            if not port and parsed.port:
                port = int(parsed.port)
    if host and port > 0:
        return f"{host}:{port}"
    return str(net.proxy_server or "").strip().lower()


def proxy_identity_key_for_account(ma: MappedAccount) -> str:
    """Khóa IP:port của tài khoản — rỗng nếu không dùng proxy."""
    if not ma.use_proxy:
        return ""
    return proxy_identity_key_for_network(ma.network)


def _extract_facebook_uid(
    account_id: str = "",
    *,
    username: str = "",
    facebook_uid: str = "",
) -> str:
    """UID số Facebook từ ``UID_…`` / username / ``facebook_uid``."""
    for raw in (facebook_uid, account_id, username):
        s = str(raw or "").strip()
        if not s:
            continue
        if s.isdigit():
            return s
        if s.upper().startswith("UID_"):
            num = s.split("_", 1)[-1]
            if num.isdigit():
                return num
    return ""


def _account_alias_ids(account_id: str, *, facebook_uid: str = "") -> set[str]:
    """Các id có thể cùng một tài khoản (import UID_ vs registry acc_)."""
    keys: set[str] = {str(account_id or "").strip()}
    uid = _extract_facebook_uid(account_id, username="", facebook_uid=facebook_uid)
    if uid.isdigit():
        keys.add(f"UID_{uid}")
        keys.add(uid)
    try:
        from src.utils.db_manager import AccountsDatabaseManager

        for rec in AccountsDatabaseManager().load_all():
            rid = str(rec.get("id") or "").strip()
            fb = str(rec.get("facebook_uid") or "").strip()
            if not rid:
                continue
            linked = {rid}
            if fb.isdigit():
                linked.add(fb)
                linked.add(f"UID_{fb}")
            if keys & linked:
                keys |= linked
    except Exception as exc:  # noqa: BLE001
        logger.debug("[Human/Proxy] alias registry lookup: {}", exc)
    return {k for k in keys if k}


def _proxy_owner_conflict(owner_id: str, offender: MappedAccount) -> bool:
    """True nếu hai id khác tài khoản thật (cùng UID/registry vẫn OK)."""
    if not owner_id:
        return False
    uid = _extract_facebook_uid(
        offender.account_id,
        username=offender.auth.username,
    )
    owner_aliases = _account_alias_ids(owner_id)
    offender_aliases = _account_alias_ids(offender.account_id, facebook_uid=uid)
    return not bool(owner_aliases & offender_aliases)


def load_registry_proxy_index() -> dict[str, str]:
    """
    Map ``IP:port`` → ``account_id`` từ ``accounts.json`` (proxy đã gắn registry).

    Returns:
        ``{proxy_identity: owner_account_id}``
    """
    index: dict[str, str] = {}
    try:
        from src.utils.db_manager import AccountsDatabaseManager
        from src.utils.proxy_check import proxy_dict_from_accounts_json, proxy_host_port_configured

        for rec in AccountsDatabaseManager().load_all():
            px = proxy_dict_from_accounts_json(rec.get("proxy"))
            if not proxy_host_port_configured(px):  # type: ignore[arg-type]
                continue
            net = proxy_dict_to_network(px)
            key = proxy_identity_key_for_network(net)
            if not key:
                continue
            owner = str(rec.get("id") or "").strip()
            if not owner:
                continue
            prev = index.get(key)
            if prev and prev != owner:
                logger.warning(
                    "[Human/Proxy] accounts.json: IP:port {} gắn cả {} và {} — cần sửa tay.",
                    key,
                    prev,
                    owner,
                )
            index[key] = owner
    except Exception as exc:  # noqa: BLE001
        logger.debug("[Human/Proxy] Không đọc registry proxy index: {}", exc)
    return index


def format_proxy_exclusive_error(
    proxy_key: str,
    owner_id: str,
    *,
    offender_id: str,
    context: str = "",
) -> str:
    """Thông báo lỗi tiếng Việt — một IP không gắn hai tài khoản."""
    ctx = f" ({context})" if context else ""
    return (
        f"IP:port {proxy_key} đã gắn tài khoản «{owner_id}» — "
        f"không được dùng cho «{offender_id}»{ctx}. "
        "Mỗi proxy (mỗi IP) chỉ một tài khoản, kể cả tab Đăng nhập / Tương tác."
    )


def assert_proxy_exclusive_among_accounts(
    accounts: list[MappedAccount],
    *,
    registry_index: dict[str, str] | None = None,
    context: str = "",
) -> None:
    """
    Raises:
        AccountProxyMappingError: Trùng IP:port trong danh sách hoặc với registry.
    """
    owner_by_key: dict[str, str] = {}
    if registry_index:
        owner_by_key.update(registry_index)
    for ma in accounts:
        if not ma.use_proxy:
            continue
        key = proxy_identity_key_for_account(ma)
        if not key:
            continue
        prev = owner_by_key.get(key)
        if prev and _proxy_owner_conflict(prev, ma):
            raise AccountProxyMappingError(
                format_proxy_exclusive_error(key, prev, offender_id=ma.account_id, context=context)
            )
        owner_by_key[key] = ma.account_id


def ensure_mapped_proxy_live(mapped: MappedAccount) -> tuple[bool, str]:
    """
    Kiểm tra proxy LIVE trước khi mở browser (giống form «Tài khoản»).

    Nếu phát hiện SOCKS5, cập nhật ``mapped.network`` với host ``socks5://…`` để Playwright
    và relay SOCKS5 dùng đúng scheme (tránh nhầm HTTP).
    """
    if not mapped.use_proxy:
        return True, "Không dùng proxy"
    px = network_to_proxy_config(mapped.network)
    host = str(px.get("host") or "").strip()
    port = int(px.get("port") or 0)
    if not host or port <= 0:
        return False, "Thiếu cấu hình proxy"
    ok, msg, scheme = check_proxy(
        host,
        port,
        user=str(px.get("user") or ""),
        password=str(px.get("pass") or ""),
        preferred_scheme=str(px.get("scheme_hint") or "") or None,
    )
    if ok and scheme != "none":
        px = apply_proxy_scheme_to_config(px, scheme)
        mapped.network = proxy_dict_to_network(px, scheme=scheme)
        logger.info(
            "[Human/Proxy] account={} — {} LIVE, host={}",
            mapped.account_id,
            scheme.upper(),
            px.get("host"),
        )
    return ok, msg


def ensure_account_dict_proxy_live(acc: dict[str, Any]) -> tuple[bool, str]:
    """
    Kiểm tra proxy LIVE cho dict ``accounts.json`` (luồng đăng lịch / đăng ngay).

    Chuẩn hóa mọi định dạng proxy, tự nhận HTTP/SOCKS5/SOCKS4 và cập nhật ``acc['proxy']`` in-place.
    """
    from src.automation.browser_factory import account_use_proxy_enabled
    from src.utils.proxy_check import (
        apply_proxy_scheme_to_config,
        check_proxy,
        proxy_dict_from_accounts_json,
        proxy_host_port_configured,
    )

    if not account_use_proxy_enabled(acc):
        return True, "Không dùng proxy"
    px = proxy_dict_from_accounts_json(acc.get("proxy"))
    acc["proxy"] = px
    host = str(px.get("host") or "").strip()
    port = int(px.get("port") or 0)
    if not proxy_host_port_configured(px):  # type: ignore[arg-type]
        return False, "use_proxy=true nhưng proxy thiếu host/port hợp lệ"
    ok, msg, scheme = check_proxy(
        host,
        port,
        user=str(px.get("user") or ""),
        password=str(px.get("pass") or ""),
        preferred_scheme=str(px.get("scheme_hint") or "") or None,
    )
    if ok and scheme != "none":
        px = apply_proxy_scheme_to_config(px, scheme)
        acc["proxy"] = px
        logger.info(
            "[Post/Proxy] account={} — {} LIVE, host={}",
            acc.get("id"),
            scheme.upper(),
            px.get("host"),
        )
    return ok, msg


def prepare_account_dict_for_browser_run(
    acc: dict[str, Any],
    *,
    require_proxy_live: bool = True,
) -> dict[str, Any]:
    """
    Chuẩn bị account dict end-to-end trước ``launch_persistent_context`` (đăng lịch, đăng ngay).

    Registry → profile có lịch sử → cookie → kiểm tra proxy LIVE (giống tab Tương tác).
    """
    from src.utils.account_browser_profile import ensure_account_browser_profile_ready

    base = dict(acc)
    enrich_account_dict_from_registry(base)
    ensure_account_browser_profile_ready(base)
    if require_proxy_live:
        ok_px, px_msg = ensure_account_dict_proxy_live(base)
        if not ok_px:
            raise ValueError(f"Proxy chưa kết nối được: {px_msg}")
    logger.info(
        "[Post] Chuẩn bị chạy account={} profile={} cookie={}",
        base.get("id"),
        base.get("portable_path") or base.get("profile_path") or "(mặc định)",
        base.get("cookie_path") or "(chưa có)",
    )
    return base


def enrich_account_dict_from_registry(acc: dict[str, Any]) -> None:
    """
    Gắn profile portable, cookie, trình duyệt từ ``accounts.json`` nếu ``id`` đã tồn tại.

    Giữ proxy trong ``acc`` (từ dòng ghép) — chỉ đồng bộ phần lưu trữ phiên giống job đăng bài.
    """
    aid = str(acc.get("id") or "").strip()
    if not aid:
        return
    try:
        from src.utils.db_manager import AccountsDatabaseManager

        db = AccountsDatabaseManager()
        rows = db.load_all()
        rec = next((r for r in rows if str(r.get("id") or "") == aid), None)
        if not rec:
            uid_from_id = _extract_facebook_uid(aid)
            if uid_from_id.isdigit():
                rec = next(
                    (r for r in rows if str(r.get("facebook_uid") or "").strip() == uid_from_id),
                    None,
                )
                if rec:
                    logger.info(
                        "[Human] Ghép registry theo UID trong id={} → registry id={}",
                        aid,
                        rec.get("id"),
                    )
        if not rec:
            fb_uid = _extract_facebook_uid(
                aid,
                username=str(acc.get("email") or ""),
                facebook_uid=str(acc.get("facebook_uid") or ""),
            )
            if fb_uid.isdigit():
                rec = next(
                    (r for r in rows if str(r.get("facebook_uid") or "").strip() == fb_uid),
                    None,
                )
                if rec:
                    logger.info(
                        "[Human] Ghép registry theo facebook_uid={} → id={}",
                        fb_uid,
                        rec.get("id"),
                    )
    except Exception as exc:  # noqa: BLE001
        logger.debug("[Human] Không đọc được accounts.json: {}", exc)
        return
    if not rec:
        return
    reg_id = str(rec.get("id") or "").strip()
    if reg_id:
        orig_id = str(acc.get("id") or "").strip()
        if orig_id and orig_id != reg_id and not acc.get("display_account_id"):
            acc["display_account_id"] = orig_id
        acc["registry_id"] = reg_id
        # Profile trên đĩa gắn ``acc_…`` trong accounts.json — không dùng ``UID_…`` từ dòng ghép.
        acc["id"] = reg_id
        name = str(acc.get("name") or "").strip()
        if not name or name.upper().startswith("UID_"):
            acc["name"] = reg_id
    portable = str(rec.get("portable_path") or rec.get("profile_path") or "").strip()
    if portable:
        acc["portable_path"] = portable
        acc["profile_path"] = portable
    cookie = str(rec.get("cookie_path") or "").strip()
    if cookie:
        acc["cookie_path"] = cookie
    bt = str(rec.get("browser_type") or "").strip()
    if bt:
        acc["browser_type"] = normalize_browser_storage(bt)
    exe = str(rec.get("browser_exe_path") or "").strip()
    if exe:
        acc["browser_exe_path"] = exe
    for key in (
        "facebook_uid",
        "email",
        "recovery_email",
        "totp_enabled",
        "password_ref",
        "totp_secret_ref",
    ):
        val = rec.get(key)
        if val is not None and str(val).strip() != "":
            acc[key] = val

    from src.utils.proxy_check import proxy_dict_from_accounts_json, proxy_host_port_configured

    reg_px = proxy_dict_from_accounts_json(rec.get("proxy"))
    if proxy_host_port_configured(reg_px):  # type: ignore[arg-type]
        cur_px = acc.get("proxy") if isinstance(acc.get("proxy"), dict) else {}
        reg_auth = bool(str(reg_px.get("user") or "").strip())
        cur_auth = bool(str(cur_px.get("user") or "").strip())
        if reg_auth and not cur_auth:
            acc["proxy"] = reg_px
            acc["use_proxy"] = bool(rec.get("use_proxy", True))
            logger.info(
                "[Human] Proxy CapSolver: dùng User/Pass từ accounts.json (host={}, auth=yes)",
                str(reg_px.get("host") or "")[:40],
            )
        elif not str(cur_px.get("host") or "").strip():
            acc["proxy"] = reg_px
            acc["use_proxy"] = bool(rec.get("use_proxy", True))
            logger.info(
                "[Human] Proxy từ accounts.json → host={} auth={}",
                str(reg_px.get("host") or "")[:40],
                "yes" if reg_auth else "no",
            )

    logger.info(
        "[Human] Đã gắn profile/cookie từ registry account={} profile={}",
        aid,
        portable or "(mặc định)",
    )


def _facebook_uid_for_storage(mapped: MappedAccount) -> str:
    """Chuẩn hóa UID số từ username / account_id (bỏ tiền tố ``UID_``)."""
    return _extract_facebook_uid(mapped.account_id, username=mapped.auth.username)


def prepare_mapped_account_for_browser_run(mapped: MappedAccount) -> dict[str, Any]:
    """
    Chuẩn bị một lượt chạy end-to-end: registry → profile có lịch sử → cookie → mkdir an toàn.

    Gọi trước ``launch_persistent_context`` (pool, worker, mở profile).
    """
    from src.utils.account_browser_profile import ensure_account_browser_profile_ready

    acc = sync_mapped_account_storage_from_registry(mapped)
    ensure_account_browser_profile_ready(acc)
    prof = str(acc.get("portable_path") or acc.get("profile_path") or "").strip()
    ck = str(acc.get("cookie_path") or mapped.cookie_path or "").strip()
    if prof:
        mapped.storage.profile_path = prof
        acc["portable_path"] = prof
        acc["profile_path"] = prof
    if ck:
        mapped.cookie_path = ck
        acc["cookie_path"] = ck
    logger.info(
        "[Human] Chuẩn bị chạy account={} (registry={}) profile={} cookie={}",
        mapped.account_id,
        acc.get("registry_id") or acc.get("id") or mapped.account_id,
        prof or "(mặc định)",
        ck or "(chưa có)",
    )
    return acc


def refresh_mapped_accounts_storage(mapped_list: list[MappedAccount]) -> None:
    """
    Đồng bộ ``profile_path`` / ``cookie_path`` mọi dòng từ registry + profile trên đĩa.

    Gọi sau restore GUI, sau pool kết thúc, trước lưu settings — lần chạy sau mở đúng profile cũ.
    """
    for ma in mapped_list:
        try:
            sync_mapped_account_storage_from_registry(ma)
        except Exception as exc:  # noqa: BLE001
            logger.debug("[Human] refresh storage {}: {}", ma.account_id, exc)


def persist_mapped_storage_to_registry(
    mapped: MappedAccount,
    acc: dict[str, Any] | None = None,
) -> None:
    """
    Ghi ``portable_path`` / ``cookie_path`` đã resolve vào ``accounts.json`` (theo registry / UID).

    Gọi sau worker đóng browser — giữ liên kết acc_… ↔ profile Firefox cho lần mở sau.
    """
    if acc is None:
        acc = mapped_account_to_account_dict(mapped)
    from src.utils.account_browser_profile import resolve_account_portable_profile

    resolve_account_portable_profile(acc)
    portable = str(
        acc.get("portable_path") or acc.get("profile_path") or mapped.storage.profile_path or ""
    ).strip()
    cookie = str(acc.get("cookie_path") or mapped.cookie_path or "").strip()
    if portable:
        mapped.storage.profile_path = portable
        acc["portable_path"] = portable
        acc["profile_path"] = portable
    if cookie:
        mapped.cookie_path = cookie
        acc["cookie_path"] = cookie
    if not portable and not cookie:
        return
    try:
        from src.utils.db_manager import AccountsDatabaseManager

        db = AccountsDatabaseManager()
        rows = db.load_all()
        reg_id = str(acc.get("registry_id") or acc.get("id") or mapped.account_id or "").strip()
        rec = next((r for r in rows if str(r.get("id") or "") == reg_id), None)
        if not rec:
            uid = _extract_facebook_uid(mapped.account_id, username=mapped.auth.username)
            if uid.isdigit():
                rec = next(
                    (r for r in rows if str(r.get("facebook_uid") or "").strip() == uid),
                    None,
                )
        if not rec:
            return
        actual_id = str(rec.get("id") or "").strip()
        updates: dict[str, Any] = {}
        if portable:
            updates["portable_path"] = portable
            updates["profile_path"] = portable
        if cookie:
            updates["cookie_path"] = cookie
        if updates:
            db.update_account_fields(actual_id, updates)
            logger.info(
                "[Human] Đã lưu profile registry id={} (hiển thị={}) → {}",
                actual_id,
                mapped.account_id,
                portable or cookie,
            )
    except Exception as exc:  # noqa: BLE001
        logger.debug("[Human] persist registry {}: {}", mapped.account_id, exc)


def sync_mapped_account_storage_from_registry(mapped: MappedAccount) -> dict[str, Any]:
    """
    Đồng bộ ``profile_path`` / ``cookie_path`` từ ``accounts.json`` (theo id hoặc ``facebook_uid``).

    Giữ ``mapped.account_id`` (UID hiển thị GUI) — chỉ cập nhật đường dẫn lưu trữ phiên thực tế.
    """
    acc = mapped_account_to_account_dict(mapped)
    from src.utils.account_browser_profile import resolve_account_portable_profile

    resolve_account_portable_profile(acc)
    portable = str(acc.get("portable_path") or acc.get("profile_path") or "").strip()
    if portable:
        mapped.storage.profile_path = portable
    bt = str(acc.get("browser_type") or "").strip()
    if bt:
        mapped.browser_type = bt
    uid = _facebook_uid_for_storage(mapped) or str(mapped.auth.username or "").strip()
    from src.services.facebook_session_persist import resolve_best_cookie_path_for_account

    mapped.cookie_path = resolve_best_cookie_path_for_account(
        acc,
        facebook_uid=uid,
        extra_candidates=[mapped.cookie_path] if mapped.cookie_path else None,
    )
    acc["cookie_path"] = mapped.cookie_path
    if portable:
        acc["portable_path"] = portable
        acc["profile_path"] = portable
    return acc


def mapped_account_to_account_dict(mapped: MappedAccount) -> dict[str, Any]:
    """
    Chuyển MappedAccount → dict tương thích ``BrowserFactory`` / ``facebook_session_recovery``.
    """
    aid = mapped.account_id
    bt = normalize_browser_storage(mapped.browser_type)
    facebook_uid = _extract_facebook_uid(aid, username=mapped.auth.username)
    px = network_to_proxy_config(mapped.network) if mapped.use_proxy else {"host": "", "port": 0, "user": "", "pass": ""}

    out: dict[str, Any] = {
        "id": aid,
        "display_account_id": aid,
        "name": aid,
        "browser_type": bt,
        "portable_path": "",
        "profile_path": "",
        "cookie_path": "",
        "proxy": px,
        "use_proxy": bool(mapped.use_proxy and px.get("host")),
        "facebook_uid": facebook_uid,
        "email": mapped.auth.email or (mapped.auth.username if "@" in str(mapped.auth.username or "") else ""),
        "recovery_email": mapped.auth.recovery_email,
        "totp_enabled": bool(mapped.auth.two_fa_secret),
        "password_ref": f"account:{aid}",
        "totp_secret_ref": f"account:{aid}",
        "_mapped_password": mapped.auth.password,
        "_mapped_totp_secret": mapped.auth.two_fa_secret,
        "_mapped_email_password": mapped.auth.email_password,
    }
    enrich_account_dict_from_registry(out)
    reg_id = str(out.get("registry_id") or out.get("id") or aid).strip()
    if not str(out.get("portable_path") or "").strip():
        out["portable_path"] = mapped.storage.profile_path or default_portable_path(reg_id, bt)
        out["profile_path"] = out["portable_path"]
    if not str(out.get("cookie_path") or "").strip():
        out["cookie_path"] = mapped.cookie_path or default_cookie_path(reg_id)
    return out


def apply_mapped_secrets_to_vault(mapped: MappedAccount) -> None:
    """
    Ghi mật khẩu/TOTP từ dòng import vào vault (thread-safe).

    Bỏ qua nếu vault đã có cùng giá trị — giảm ghi file khi nhiều worker chạy song song.
    """
    from src.utils.account_credentials import load_account_credential_bundle, set_account_credentials

    aid = mapped.account_id
    if not aid:
        return
    kwargs: dict[str, Any] = {}
    if mapped.auth.password:
        kwargs["password"] = mapped.auth.password
    if mapped.auth.two_fa_secret:
        kwargs["totp_secret"] = mapped.auth.two_fa_secret
    if mapped.auth.recovery_email:
        kwargs["recovery_email"] = mapped.auth.recovery_email
    if not kwargs:
        return

    stub = {
        "id": aid,
        "password_ref": f"account:{aid}",
        "totp_secret_ref": f"account:{aid}",
    }
    bundle = load_account_credential_bundle(stub)
    if bundle:
        if "password" in kwargs and bundle.password == kwargs["password"]:
            del kwargs["password"]
        if "totp_secret" in kwargs and bundle.totp_secret == kwargs["totp_secret"]:
            del kwargs["totp_secret"]
        if "recovery_email" in kwargs and bundle.recovery_email == kwargs["recovery_email"]:
            del kwargs["recovery_email"]
    if kwargs:
        set_account_credentials(aid, **kwargs)


def _account_id_from_auth(auth: MappedAccountAuth, index: int) -> str:
    u = auth.username.strip()
    if u.isdigit():
        return f"UID_{u}"
    if re.fullmatch(r"UID_\d+", u, re.I):
        return u.upper().replace("uid_", "UID_") if u.lower().startswith("uid_") else u
    safe = re.sub(r"[^\w\-]", "_", u)[:48] or f"acc_{index}"
    return f"import_{safe}"


def count_unique_proxy_servers(accounts: list[MappedAccount]) -> int:
    """Số IP:port proxy khác nhau — dùng kiểm tra đủ luồng song song."""
    return len(proxy_identity_groups(accounts))


def proxy_server_groups(accounts: list[MappedAccount]) -> dict[str, list[str]]:
    """Alias — nhóm theo IP:port (không phải chuỗi URL đầy đủ)."""
    return proxy_identity_groups(accounts)


def proxy_identity_groups(accounts: list[MappedAccount]) -> dict[str, list[str]]:
    """Nhóm ``account_id`` theo khóa IP:port."""
    groups: dict[str, list[str]] = {}
    for ma in accounts:
        key = proxy_identity_key_for_account(ma)
        if not key:
            continue
        groups.setdefault(key, []).append(ma.account_id)
    return groups


def duplicate_proxy_assignments(accounts: list[MappedAccount]) -> dict[str, list[str]]:
    """IP:port dùng chung bởi ≥2 tài khoản — ``{ip:port: [account_id, ...]}``."""
    return {k: v for k, v in proxy_identity_groups(accounts).items() if len(v) > 1}


class DeadProxyLine(TypedDict):
    """Một dòng proxy không LIVE sau khi check."""

    line_no: int
    proxy_line: str
    error: str


def _check_one_proxy_line(line: str, *, line_no: int, timeout: float) -> tuple[int, str, bool, str, str, dict[str, Any]]:
    """
    Kiểm tra một dòng proxy.

    Returns:
        ``(line_no, display_line, ok, ip_or_error, scheme, parsed_px)``.
    """
    raw = str(line or "").strip()
    if not raw:
        return line_no, raw, False, "Dòng trống", "none", {}
    try:
        ok, msg, scheme, px = check_proxy_line(raw, timeout=timeout)
        display = format_proxy_line(px, scheme) if ok and scheme != "none" else raw
        if ok:
            return line_no, display, True, str(msg), str(scheme), px
        return line_no, raw, False, str(msg).split("\n")[0][:200], "none", {}
    except Exception as exc:  # noqa: BLE001
        return line_no, raw, False, str(exc)[:200], "none", {}


def filter_lines_by_live_proxy(
    account_lines: list[str],
    proxy_lines: list[str],
    *,
    max_workers: int = 6,
    timeout: float = 18.0,
) -> tuple[list[str], list[str], list[DeadProxyLine], dict[str, int]]:
    """
    Giữ các cặp dòng (TK, proxy) mà proxy LIVE; bỏ proxy die / parse lỗi.

    Chỉ ghép theo index ``i`` với ``i < min(len(account), len(proxy))``.
    Dòng TK hoặc proxy thừa (không có cặp) được giữ nguyên nếu proxy thừa LIVE,
    hoặc bỏ nếu proxy thừa die.

    Returns:
        ``(live_accounts, live_proxies, dead_report)``.
    """
    acc_in = [ln.strip() for ln in account_lines if str(ln or "").strip()]
    px_in = [ln.strip() for ln in proxy_lines if str(ln or "").strip()]
    if not px_in:
        return acc_in, [], [], {}

    pair_n = min(len(acc_in), len(px_in))
    solo_px = px_in[pair_n:]
    solo_acc = acc_in[pair_n:]

    dead: list[DeadProxyLine] = []
    scheme_counts: dict[str, int] = {}
    live_by_index: dict[int, tuple[str, str, str, dict[str, Any]]] = {}

    def _check_indexed(idx: int, px_line: str) -> tuple[int, str, bool, str, str, dict[str, Any]]:
        return _check_one_proxy_line(px_line, line_no=idx + 1, timeout=timeout)

    workers = max(1, min(int(max_workers), 12, len(px_in)))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_check_indexed, i, px_in[i]): i for i in range(len(px_in))
        }
        for fut in as_completed(futures):
            line_no, px_line, ok, msg, scheme, px = fut.result()
            idx = line_no - 1
            if ok:
                live_by_index[idx] = (px_line, msg, scheme, px)
                scheme_counts[str(scheme)] = scheme_counts.get(str(scheme), 0) + 1
            else:
                dead.append(
                    {
                        "line_no": line_no,
                        "proxy_line": px_line[:120],
                        "error": msg,
                    }
                )

    live_acc: list[str] = []
    live_px: list[str] = []
    used_proxy_keys: set[str] = set()
    for i in range(pair_n):
        if live_by_index.get(i) is None:
            continue
        px_line, _msg, _scheme, px_parsed = live_by_index[i]
        try:
            net = proxy_dict_to_network(px_parsed, scheme=_scheme if _scheme != "none" else None)
            pkey = proxy_identity_key_for_network(net)
        except Exception:
            pkey = ""
        if pkey:
            if pkey in used_proxy_keys:
                dead.append(
                    {
                        "line_no": i + 1,
                        "proxy_line": px_line[:120],
                        "error": f"Trùng IP:port {pkey} — chỉ giữ tài khoản ghép đầu tiên",
                    }
                )
                logger.warning(
                    "[Human/Proxy] Bỏ cặp dòng {} — proxy {} đã gắn tài khoản trước đó.",
                    i + 1,
                    pkey,
                )
                continue
            used_proxy_keys.add(pkey)
        live_acc.append(acc_in[i])
        live_px.append(px_line)

    for j, _px_line in enumerate(solo_px, start=pair_n):
        hit = live_by_index.get(j)
        if hit:
            live_px.append(hit[0])

    live_acc.extend(solo_acc)

    logger.info(
        "[Human/Proxy] Check LIVE: giữ {}/{} proxy, bỏ {} die",
        len(live_px),
        len(px_in),
        len(dead),
    )
    return live_acc, live_px, dead, scheme_counts


def accounts_without_proxy(accounts: list[MappedAccount]) -> list[str]:
    """Danh sách ``account_id`` thiếu cấu hình proxy."""
    out: list[str] = []
    for ma in accounts:
        if not ma.use_proxy:
            out.append(ma.account_id)
            continue
        if not str(ma.network.proxy_server or "").strip():
            out.append(ma.account_id)
    return out


def map_accounts_with_proxies(
    account_lines: list[str],
    proxy_lines: list[str],
    *,
    max_concurrent: int,
    browser_type: str = "firefox",
    persist_secrets: bool = True,
) -> list[MappedAccount]:
    """
    Ghép dòng i account với dòng i proxy.

    Raises:
        AccountProxyMappingError: Thiếu proxy hoặc số proxy < max_concurrent.
    """
    n_acc = len(account_lines)
    n_px = len(proxy_lines)
    mc = max(1, int(max_concurrent))

    if n_acc == 0:
        raise AccountProxyMappingError("Danh sách tài khoản trống.")
    if n_px == 0:
        raise AccountProxyMappingError("Danh sách proxy trống.")
    if n_px < n_acc:
        logger.warning(
            "Số proxy ({}) < số tài khoản ({}). Chỉ ghép {} cặp đầu.",
            n_px,
            n_acc,
            n_px,
        )

    pair_count = min(n_acc, n_px)
    mapped_list: list[MappedAccount] = []
    seen_ids: dict[str, int] = {}
    seen_proxy_keys: dict[str, str] = {}
    registry_index = load_registry_proxy_index()

    for i in range(pair_count):
        try:
            auth = parse_account_line(account_lines[i], default_browser=browser_type)
        except ValueError as exc:
            raise AccountProxyMappingError(f"Dòng tài khoản {i + 1}: {exc}") from exc
        try:
            network = parse_proxy_line_to_network(proxy_lines[i])
        except ValueError as exc:
            raise AccountProxyMappingError(f"Dòng proxy {i + 1}: {exc}") from exc

        pkey = proxy_identity_key_for_network(network)
        if pkey:
            if pkey in seen_proxy_keys:
                raise AccountProxyMappingError(
                    format_proxy_exclusive_error(
                        pkey,
                        seen_proxy_keys[pkey],
                        offender_id=_account_id_from_auth(auth, i),
                        context=f"dòng proxy {i + 1} trùng dòng trước",
                    )
                )
            reg_owner = registry_index.get(pkey)
            if reg_owner:
                cand = _account_id_from_auth(auth, i)
                if reg_owner not in _account_alias_ids(
                    cand,
                    facebook_uid=_extract_facebook_uid(cand, username=auth.username),
                ):
                    raise AccountProxyMappingError(
                        format_proxy_exclusive_error(
                            pkey,
                            reg_owner,
                            offender_id=cand,
                            context="đã gắn trong accounts.json",
                        )
                    )

        base_aid = _account_id_from_auth(auth, i)
        n_dup = seen_ids.get(base_aid, 0) + 1
        seen_ids[base_aid] = n_dup
        aid = base_aid if n_dup == 1 else f"{base_aid}_L{n_dup}"
        if n_dup > 1:
            logger.warning(
                "UID/username trùng «{}» ở dòng {} — dùng id riêng {} để không chia sẻ profile.",
                base_aid,
                i + 1,
                aid,
            )
        bt = normalize_browser_storage(browser_type)
        ma = MappedAccount(
            account_id=aid,
            auth=auth,
            network=network,
            storage=MappedAccountStorage(profile_path=default_portable_path(aid, bt)),
            browser_type=bt,
            cookie_path=default_cookie_path(aid),
            use_proxy=True,
            status="pending",
            grid_slot_index=i % mc,
        )
        if persist_secrets:
            apply_mapped_secrets_to_vault(ma)
        try:
            sync_mapped_account_storage_from_registry(ma)
        except Exception as sync_exc:  # noqa: BLE001
            logger.warning("[Human] Sync registry sau ghép dòng {}: {}", aid, sync_exc)
        if pkey:
            seen_proxy_keys[pkey] = aid
        mapped_list.append(ma)

    assert_proxy_exclusive_among_accounts(mapped_list, context="sau ghép dòng")

    dups = duplicate_proxy_assignments(mapped_list)
    if dups:
        lines: list[str] = []
        for px_key, aids in list(dups.items())[:6]:
            lines.append(f"• {px_key} → {', '.join(aids)}")
        raise AccountProxyMappingError(
            "Trùng IP:port giữa các tài khoản (mỗi IP chỉ một tài khoản):\n" + "\n".join(lines)
        )
    missing = accounts_without_proxy(mapped_list)
    if missing:
        logger.warning("[Human/Proxy] Thiếu proxy: {}", ", ".join(missing[:12]))

    logger.info(
        "Đã ghép {} tài khoản | {} proxy riêng | max_concurrent={}",
        len(mapped_list),
        count_unique_proxy_servers(mapped_list),
        mc,
    )
    return mapped_list


def _collect_used_proxy_keys(
    all_accounts: list[MappedAccount],
    *,
    exclude_account_ids: set[str],
    registry_index: dict[str, str] | None = None,
) -> dict[str, str]:
    """``{ip:port → account_id}`` — proxy đã gắn (trừ tài khoản đang đổi proxy)."""
    used: dict[str, str] = {}
    exclude_aliases: set[str] = set()
    for aid in exclude_account_ids:
        exclude_aliases |= _account_alias_ids(aid)
    if registry_index:
        for key, owner in registry_index.items():
            if owner in exclude_aliases:
                continue
            used.setdefault(key, owner)
    for ma in all_accounts:
        if ma.account_id in exclude_account_ids:
            continue
        if not ma.use_proxy:
            continue
        key = proxy_identity_key_for_account(ma)
        if key:
            used[key] = ma.account_id
    return used


def reassign_proxies_from_pool(
    targets: list[MappedAccount],
    *,
    all_accounts: list[MappedAccount],
    proxy_lines: list[str],
) -> dict[str, Any]:
    """
    Gán proxy mới từ danh sách (tab Đăng nhập) cho các tài khoản lỗi proxy.

    - Không trùng IP:port với TK khác (login + tương tác + accounts.json).
    - Bỏ qua proxy hiện tại của TK (đã lỗi) và proxy đã dùng bởi TK khác.

    Returns:
        ``{updated: [account_id], skipped: [(account_id, reason)], assignments: {id: proxy_line}}``
    """
    if not targets:
        return {"updated": [], "skipped": [], "assignments": {}}
    pool = [ln.strip() for ln in proxy_lines if str(ln or "").strip()]
    if not pool:
        raise AccountProxyMappingError("Danh sách proxy trống — nhập ở tab Đăng nhập.")

    target_ids = {ma.account_id for ma in targets}
    registry_index = load_registry_proxy_index()
    used_keys = _collect_used_proxy_keys(
        all_accounts,
        exclude_account_ids=target_ids,
        registry_index=registry_index,
    )

    updated: list[str] = []
    skipped: list[tuple[str, str]] = []
    assignments: dict[str, str] = {}

    for ma in targets:
        current_key = proxy_identity_key_for_account(ma) if ma.use_proxy else ""
        assigned = False
        for raw_line in pool:
            try:
                net = parse_proxy_line_to_network(raw_line)
            except ValueError as exc:
                logger.debug("[Human/Proxy] Bỏ qua dòng proxy: {}", exc)
                continue
            pkey = proxy_identity_key_for_network(net)
            if not pkey:
                continue
            if pkey == current_key:
                continue
            owner = used_keys.get(pkey)
            if owner:
                aliases = _account_alias_ids(
                    ma.account_id,
                    facebook_uid=ma.auth.username if ma.auth.username.isdigit() else "",
                )
                if owner not in aliases:
                    continue
            ma.network = net
            ma.use_proxy = True
            ma.status = "pending"
            ma.status_detail = "Đã đổi proxy — chờ chạy lại"
            used_keys[pkey] = ma.account_id
            updated.append(ma.account_id)
            assignments[ma.account_id] = raw_line[:120]
            assigned = True
            logger.info(
                "[Human/Proxy] Đổi proxy account={} → {}",
                ma.account_id,
                pkey,
            )
            break
        if not assigned:
            skipped.append(
                (
                    ma.account_id,
                    "Không còn proxy trống trong list (trùng IP hoặc hết dòng).",
                )
            )

    if updated:
        try:
            assert_proxy_exclusive_among_accounts(
                all_accounts,
                registry_index=registry_index,
                context="sau cập nhật proxy",
            )
        except AccountProxyMappingError:
            pass

    return {"updated": updated, "skipped": skipped, "assignments": assignments}


def persist_mapped_proxy_to_accounts_json(mapped: MappedAccount) -> bool:
    """Ghi proxy mới vào ``accounts.json`` nếu có bản ghi khớp id/UID."""
    from src.utils.db_manager import AccountsDatabaseManager

    aid = str(mapped.account_id or "").strip()
    if not aid:
        return False
    try:
        db = AccountsDatabaseManager()
        rows = db.load_all()
    except Exception as exc:  # noqa: BLE001
        logger.debug("persist_mapped_proxy: {}", exc)
        return False
    rec = next((r for r in rows if str(r.get("id") or "").strip() == aid), None)
    if rec is None:
        uid = str(mapped.auth.username or "").strip()
        if uid.isdigit():
            rec = next(
                (r for r in rows if str(r.get("facebook_uid") or "").strip() == uid),
                None,
            )
    if rec is None:
        return False
    reg_id = str(rec.get("id") or "").strip()
    if not reg_id:
        return False
    px = (
        network_to_proxy_config(mapped.network)
        if mapped.use_proxy
        else {"host": "", "port": 0, "user": "", "pass": ""}
    )
    try:
        db.update_account_fields(
            reg_id,
            {
                "proxy": px,
                "use_proxy": bool(mapped.use_proxy and str(px.get("host") or "").strip()),
            },
        )
        return True
    except Exception as exc:  # noqa: BLE001
        logger.warning("[Human/Proxy] Không ghi accounts.json id={}: {}", reg_id, exc)
        return False


class ExportMappedRegistryResult(TypedDict):
    """Kết quả đưa tài khoản tab Tương tác vào ``accounts.json``."""

    added: list[str]
    updated: list[str]
    skipped: list[tuple[str, str]]


def _registry_profile_id_from_path(portable: str) -> str:
    """Lấy ``acc_…`` từ cuối ``portable_path`` nếu có."""
    tail = str(portable or "").replace("\\", "/").rstrip("/").split("/")[-1].strip()
    if tail.startswith("acc_"):
        return tail
    return ""


def _find_registry_row_for_mapped(
    rows: list[dict[str, Any]],
    mapped: MappedAccount,
) -> dict[str, Any] | None:
    """Tìm bản ghi ``accounts.json`` theo ``id`` hoặc ``facebook_uid``."""
    aid = str(mapped.account_id or "").strip()
    if aid:
        hit = next((r for r in rows if str(r.get("id") or "").strip() == aid), None)
        if hit is not None:
            return hit
    uid = mapped.display_uid()
    if uid.isdigit():
        return next(
            (r for r in rows if str(r.get("facebook_uid") or "").strip() == uid),
            None,
        )
    return None


def mapped_account_eligible_for_registry_export(mapped: MappedAccount) -> tuple[bool, str]:
    """Chỉ xuất TK đã có phiên cookie (đăng nhập OK hoặc file cookie hợp lệ)."""
    if mapped.status in ("login_ok", "success"):
        return True, ""
    try:
        from src.services.facebook_session_persist import cookie_file_has_session

        sync_mapped_account_storage_from_registry(mapped)
        ck = str(mapped.cookie_path or "").strip()
        if ck and cookie_file_has_session(ck):
            return True, ""
    except Exception as exc:  # noqa: BLE001
        logger.debug("[Human/Export] Kiểm tra cookie {}: {}", mapped.account_id, exc)
    st = mapped.status or "pending"
    return False, f"Trạng thái «{st}» — chưa có cookie phiên đăng nhập"


def mapped_account_to_registry_record(
    mapped: MappedAccount,
    *,
    existing: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Chuyển ``MappedAccount`` → bản ghi ``accounts.json`` (profile/cookie/proxy có sẵn trên đĩa).

    ``import_type=folder`` để ``upsert`` không tạo profile Playwright mới.
    """
    apply_mapped_secrets_to_vault(mapped)
    sync_mapped_account_storage_from_registry(mapped)
    acc = mapped_account_to_account_dict(mapped)
    for key in list(acc.keys()):
        if str(key).startswith("_mapped_"):
            acc.pop(key, None)

    bt = normalize_browser_storage(str(acc.get("browser_type") or mapped.browser_type or "firefox"))
    portable = str(
        acc.get("portable_path") or acc.get("profile_path") or mapped.storage.profile_path or ""
    ).strip()
    if not portable:
        portable = default_portable_path(str(acc.get("id") or mapped.account_id), bt)
    cookie = str(acc.get("cookie_path") or mapped.cookie_path or "").strip()
    if not cookie:
        cookie = default_cookie_path(str(acc.get("id") or mapped.account_id))

    reg_id = str(acc.get("id") or "").strip()
    if existing and str(existing.get("id") or "").strip():
        reg_id = str(existing.get("id") or "").strip()
    elif not reg_id or reg_id.startswith("UID_"):
        from_path = _registry_profile_id_from_path(portable)
        reg_id = from_path or reg_id or f"acc_{uuid.uuid4().hex[:10]}"
        if reg_id.startswith("UID_"):
            reg_id = f"acc_{uuid.uuid4().hex[:10]}"

    uid = str(acc.get("facebook_uid") or "").strip()
    if not uid.isdigit():
        disp = mapped.display_uid()
        uid = disp if disp.isdigit() else ""

    name = str(acc.get("name") or "").strip()
    if existing:
        ex_name = str(existing.get("name") or "").strip()
        if ex_name and ex_name not in ("Tài khoản mới", mapped.account_id) and not ex_name.startswith("UID_"):
            name = ex_name
    if not name or name.startswith("UID_") or name == mapped.account_id:
        if uid.isdigit():
            name = uid
        elif mapped.auth.email and "@" in mapped.auth.email:
            name = mapped.auth.email.split("@", 1)[0]
        else:
            name = reg_id

    px = acc.get("proxy") if isinstance(acc.get("proxy"), dict) else network_to_proxy_config(mapped.network)
    use_px = bool(acc.get("use_proxy", mapped.use_proxy) and str(px.get("host") or "").strip())

    from src.utils.account_credentials import default_password_ref, default_totp_secret_ref

    vault_id = str(mapped.account_id or reg_id).strip()
    rec: dict[str, Any] = {
        "id": reg_id,
        "name": name,
        "browser_type": bt,
        "portable_path": portable,
        "profile_path": portable,
        "cookie_path": cookie,
        "proxy": px,
        "use_proxy": use_px,
        "import_type": "folder",
        "facebook_uid": uid,
        "email": str(mapped.auth.email or acc.get("email") or "").strip(),
        "recovery_email": str(mapped.auth.recovery_email or acc.get("recovery_email") or "").strip(),
        "totp_enabled": bool(mapped.auth.two_fa_secret or acc.get("totp_enabled")),
        "password_ref": str(acc.get("password_ref") or default_password_ref(vault_id)),
        "totp_secret_ref": str(acc.get("totp_secret_ref") or default_totp_secret_ref(vault_id)),
        "session_status": "active",
        "login_status": "active",
        "status": "success" if mapped.status in ("login_ok", "success") else str(mapped.status or "pending"),
        "notes": str((existing or {}).get("notes") or acc.get("notes") or "").strip(),
    }
    if existing:
        for key in (
            "schedule_time",
            "topic",
            "content_style",
            "post_image_path",
            "last_post_at",
            "browser_exe_path",
        ):
            val = existing.get(key)
            if val is not None and str(val).strip() != "":
                rec[key] = val
    return rec


def export_mapped_accounts_to_registry(
    mapped_list: list[MappedAccount],
    *,
    db: Any | None = None,
) -> ExportMappedRegistryResult:
    """
    Ghi danh sách tài khoản tab Tương tác vào ``accounts.json`` để dùng tab «Tài khoản» / lịch đăng.

    Returns:
        ``{added, updated, skipped}`` — id registry sau khi ghi.
    """
    from src.utils.db_manager import AccountsDatabaseManager

    manager = db or AccountsDatabaseManager()
    added: list[str] = []
    updated: list[str] = []
    skipped: list[tuple[str, str]] = []

    try:
        rows = manager.load_all()
    except Exception as exc:  # noqa: BLE001
        raise AccountProxyMappingError(f"Không đọc được accounts.json: {exc}") from exc

    for mapped in mapped_list:
        ok, reason = mapped_account_eligible_for_registry_export(mapped)
        if not ok:
            skipped.append((mapped.account_id, reason))
            continue
        existing = _find_registry_row_for_mapped(rows, mapped)
        try:
            rec = mapped_account_to_registry_record(mapped, existing=existing)
            manager.validate_account(rec)
            manager.upsert(rec)  # type: ignore[arg-type]
            reg_id = str(rec.get("id") or "").strip()
            if existing:
                updated.append(reg_id)
            else:
                added.append(reg_id)
            rows = manager.load_all()
            logger.info(
                "[Human/Export] {} → accounts.json id={} uid={}",
                "Cập nhật" if existing else "Thêm",
                reg_id,
                rec.get("facebook_uid") or mapped.display_uid(),
            )
        except Exception as exc:  # noqa: BLE001
            skipped.append((mapped.account_id, str(exc)[:160]))
            logger.warning("[Human/Export] Bỏ qua {}: {}", mapped.account_id, exc)

    return {"added": added, "updated": updated, "skipped": skipped}


def map_from_text_files(
    accounts_path: str | Path,
    proxies_path: str | Path,
    *,
    max_concurrent: int,
    browser_type: str = "firefox",
) -> list[MappedAccount]:
    """Đọc hai file và ghép."""
    return map_accounts_with_proxies(
        read_lines_file(accounts_path),
        read_lines_file(proxies_path),
        max_concurrent=max_concurrent,
        browser_type=browser_type,
    )
