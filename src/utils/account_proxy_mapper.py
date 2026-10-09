"""
Ghép danh sách tài khoản với proxy theo dòng (dòng i ↔ dòng i).

Ràng buộc: số proxy ≥ số luồng đồng thời; **mỗi IP:port chỉ một tài khoản**
(đăng nhập hay chưa — không chia sẻ proxy giữa hai UID).
"""

from __future__ import annotations

import html as html_module
import json
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


# id → nhãn hiện trên ô chọn. Thứ tự này là thứ tự trong combobox.
ACCOUNT_LINE_FORMAT_LABELS: tuple[tuple[str, str], ...] = (
    (
        "mail",
        "uid | pass | 2FA | mail | pass mail | mail khôi phục",
    ),
    (
        "cookie",
        "UID | mật khẩu | 2FA | cookie | mail khôi phục | mật khẩu mail",
    ),
    (
        "auto",
        "Tự nhận theo từng dòng",
    ),
)


def account_line_format_label(fmt: str) -> str:
    """Nhãn hiển thị của một id định dạng. Id lạ trả về nhãn mail."""
    key = str(fmt or "").strip().lower()
    for fid, label in ACCOUNT_LINE_FORMAT_LABELS:
        if fid == key:
            return label
    return ACCOUNT_LINE_FORMAT_LABELS[0][1]


def normalize_account_line_format(fmt: str) -> str:
    """Chuẩn hóa id định dạng. Giá trị lạ → ``mail`` (định dạng cũ)."""
    key = str(fmt or "").strip().lower()
    if key in {fid for fid, _label in ACCOUNT_LINE_FORMAT_LABELS}:
        return key
    return "mail"


def _looks_like_email(value: str) -> bool:
    text = str(value or "").strip()
    return bool(re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", text))


def _looks_like_cookie(value: str) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    low = text.lower()
    if low.startswith("{") or low.startswith("["):
        return True
    if "c_user=" in low or "xs=" in low:
        return True
    return ";" in text and "=" in text


def _split_delimited(line: str) -> list[str]:
    """Tách dòng theo ``|``, tab hoặc ``;`` — không cắt bớt trường."""
    raw = str(line or "").strip()
    if not raw:
        raise ValueError("Dòng tài khoản rỗng.")
    if "|" in raw:
        return [p.strip() for p in raw.split("|")]
    if "\t" in raw:
        return [p.strip() for p in raw.split("\t")]
    if ";" in raw:
        return [p.strip() for p in raw.split(";")]
    return [raw]


def detect_account_line_format(parts: list[str]) -> str:
    """
    Nhận định dạng một dòng khi người dùng chọn «Tự nhận».

    Trường thứ 4 là email → định dạng mail. Không phải email (cookie, JSON, ``c_user``) → cookie.
    """
    if len(parts) >= 4 and parts[3] and not _looks_like_email(parts[3]):
        if _looks_like_cookie(parts[3]) or not _looks_like_email(parts[3]):
            return "cookie"
    return "mail"


def _parse_cookie_account_parts(parts: list[str]) -> MappedAccountAuth:
    """
    ``UID|mật khẩu|2FA|cookie|mail khôi phục|mật khẩu mail``.

    Cookie có thể chứa ``|``. Khi đó mail (có ``@``) và mật khẩu mail nằm ở cuối dòng.
    """
    if not parts or not parts[0]:
        raise ValueError("Thiếu UID/username.")
    username = parts[0]
    password = parts[1] if len(parts) > 1 else ""
    totp = parts[2] if len(parts) > 2 else ""
    rest = parts[3:]
    recovery = ""
    email_pass = ""
    cookie = ""
    email_idx = next(
        (i for i, part in enumerate(rest) if _looks_like_email(part) and not _looks_like_cookie(part)),
        None,
    )
    cookie_idx = next((i for i, part in enumerate(rest) if _looks_like_cookie(part)), None)
    if email_idx is not None and cookie_idx is not None and cookie_idx < email_idx:
        cookie = "|".join(rest[cookie_idx:email_idx]).strip("|")
        recovery = rest[email_idx]
        if email_idx + 1 < len(rest):
            email_pass = rest[email_idx + 1]
    elif email_idx is not None and cookie_idx is not None and cookie_idx > email_idx:
        recovery = rest[email_idx]
        if email_idx + 1 < cookie_idx:
            email_pass = rest[email_idx + 1]
        cookie = "|".join(rest[cookie_idx:]).strip("|")
    elif email_idx is not None:
        recovery = rest[email_idx]
        if email_idx + 1 < len(rest):
            email_pass = rest[email_idx + 1]
        cookie = "|".join(rest[:email_idx]).strip("|")
    else:
        cookie = "|".join(rest).strip("|")
    cookie, recovery = _separate_cookie_and_recovery(cookie, recovery)
    return MappedAccountAuth(
        username=username,
        password=password,
        two_fa_secret=totp,
        email="",
        email_password=email_pass,
        recovery_email=recovery,
        imported_cookie=cookie,
    )


def _separate_cookie_and_recovery(cookie: str, recovery: str) -> tuple[str, str]:
    """Email khôi phục không được nằm trong chuỗi cookie, và ngược lại."""
    cookie = str(cookie or "").strip()
    recovery = str(recovery or "").strip()
    if _looks_like_cookie(recovery) and not _looks_like_cookie(cookie):
        cookie, recovery = recovery, ""
    if not cookie:
        return "", recovery
    if "|" in cookie:
        kept: list[str] = []
        for part in cookie.split("|"):
            piece = part.strip()
            if not piece:
                continue
            if _looks_like_email(piece) and not _looks_like_cookie(piece):
                if not recovery:
                    recovery = piece
                continue
            kept.append(piece)
        cookie = "|".join(kept).strip("|")
    if _looks_like_email(cookie) and not _looks_like_cookie(cookie):
        if not recovery:
            recovery = cookie
        cookie = ""
    return cookie, recovery


def cookie_header_from_file(cookie_path: str) -> str:
    """Đọc file cookie Playwright thành chuỗi ``c_user=…; xs=…`` để hiện trên form."""
    raw_path = str(cookie_path or "").strip()
    if not raw_path:
        return ""
    path = Path(raw_path)
    if not path.is_absolute():
        from src.utils.paths import project_root

        path = project_root() / path
    if not path.is_file():
        return ""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return ""
    items = payload.get("cookies") if isinstance(payload, dict) else payload
    if not isinstance(items, list):
        return ""
    pairs: list[str] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        if not name:
            continue
        pairs.append(f"{name}={item.get('value') or ''}")
    return "; ".join(pairs)


def playwright_cookies_from_import(raw: str) -> list[dict[str, Any]]:
    """
    Đổi cookie dán (JSON Playwright hoặc ``c_user=…; xs=…``) thành mảng ``add_cookies``.

    Không ghi log nội dung cookie.
    """
    text = str(raw or "").strip()
    if not text:
        return []
    parsed: Any = None
    if text[:1] in "{[":
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            parsed = None
    items: list[Any] = []
    if isinstance(parsed, dict) and isinstance(parsed.get("cookies"), list):
        items = list(parsed["cookies"])
    elif isinstance(parsed, list):
        items = list(parsed)
    out: list[dict[str, Any]] = []
    if items:
        for item in items:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "").strip()
            value = str(item.get("value") or "")
            if not name:
                continue
            domain = str(item.get("domain") or ".facebook.com").strip() or ".facebook.com"
            path = str(item.get("path") or "/").strip() or "/"
            cookie: dict[str, Any] = {
                "name": name,
                "value": value,
                "domain": domain,
                "path": path,
                "secure": bool(item.get("secure", True)),
                "httpOnly": bool(item.get("httpOnly", False)),
            }
            same = str(item.get("sameSite") or "None")
            if same not in {"Strict", "Lax", "None"}:
                same = "None"
            cookie["sameSite"] = same
            out.append(cookie)
        return out
    for part in text.split(";"):
        piece = part.strip()
        if "=" not in piece:
            continue
        name, value = piece.split("=", 1)
        name = name.strip()
        value = value.strip()
        if not name:
            continue
        out.append(
            {
                "name": name,
                "value": value,
                "domain": ".facebook.com",
                "path": "/",
                "secure": True,
                "httpOnly": name.lower() in {"xs", "fr", "datr", "sb"},
                "sameSite": "None",
            }
        )
    return out


def write_imported_cookie_file(cookie_path: str, raw: str) -> int:
    """
    Ghi cookie nhập từ dòng nick vào file storage Playwright.

    Returns:
        Số cookie đã ghi. ``0`` nếu chuỗi không đọc được.
    """
    cookies = playwright_cookies_from_import(raw)
    if not cookies:
        return 0
    from src.utils.paths import project_root

    path = Path(cookie_path)
    if not path.is_absolute():
        path = project_root() / path
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"cookies": cookies, "origins": []}
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return len(cookies)


def attach_imported_cookie(ma: MappedAccount) -> None:
    """Ghi cookie của dòng nick ra ``cookie_path`` rồi xóa chuỗi thô khỏi bộ nhớ."""
    raw = str(ma.auth.imported_cookie or "").strip()
    if not raw or not ma.cookie_path:
        return
    raw, recovery = _separate_cookie_and_recovery(raw, ma.auth.recovery_email)
    if recovery and not ma.auth.recovery_email:
        ma.auth.recovery_email = recovery
    if ma.auth.email and ma.auth.email == ma.auth.recovery_email and not _looks_like_cookie(ma.auth.email):
        ma.auth.email = ""
    if not raw or not _looks_like_cookie(raw):
        ma.auth.imported_cookie = ""
        ma.login_via_cookie = False
        return
    ma.auth.imported_cookie = raw
    try:
        count = write_imported_cookie_file(ma.cookie_path, raw)
    except OSError as exc:
        logger.warning("[Human] Không ghi được cookie nhập cho {}: {}", ma.account_id, exc)
        return
    ma.auth.imported_cookie = ""
    if count <= 0:
        ma.login_via_cookie = False
        logger.warning(
            "[Human] Dòng nick của {} có cookie nhưng không đọc được — sẽ đăng nhập bằng mật khẩu.",
            ma.account_id,
        )
        return
    from src.services.facebook_session_persist import cookie_file_has_session

    if cookie_file_has_session(ma.cookie_path):
        ma.login_via_cookie = True
        ma.status_detail = "Sẵn sàng đăng nhập bằng cookie"
        logger.info("[Human] {} sẽ đăng nhập bằng cookie ({} cookie)", ma.account_id, count)
        return
    ma.login_via_cookie = False
    logger.info("[Human] Đã nạp {} cookie cho {} nhưng thiếu c_user — đăng nhập bằng mật khẩu", count, ma.account_id)


def apply_imported_cookies_to_accounts(
    accounts: list[MappedAccount],
    account_lines: list[str],
    *,
    account_format: str = "cookie",
) -> int:
    """
    Gắn cookie từ các dòng nick vào tài khoản cùng UID.

    Dùng khi người dùng chọn định dạng cookie rồi bấm đăng nhập,
    kể cả khi bảng đã ghép từ trước.

    Returns:
        Số tài khoản được đánh dấu đăng nhập bằng cookie.
    """
    by_key: dict[str, MappedAccount] = {}
    for ma in accounts:
        uid = _extract_facebook_uid(ma.account_id, username=ma.auth.username)
        if uid:
            by_key[uid] = ma
        name = str(ma.auth.username or "").strip()
        if name:
            by_key.setdefault(name, ma)
    attached = 0
    for line in account_lines:
        raw = str(line or "").strip()
        if not raw or raw.startswith("#"):
            continue
        try:
            auth = parse_account_line(raw, account_format=account_format)
        except ValueError:
            continue
        if not str(auth.imported_cookie or "").strip():
            continue
        uid = _extract_facebook_uid("", username=auth.username)
        ma = by_key.get(uid) or by_key.get(auth.username)
        if ma is None:
            continue
        ma.auth.imported_cookie = auth.imported_cookie
        if auth.password and not ma.auth.password:
            ma.auth.password = auth.password
        if auth.two_fa_secret and not ma.auth.two_fa_secret:
            ma.auth.two_fa_secret = auth.two_fa_secret
        if not ma.cookie_path:
            ma.cookie_path = default_cookie_path(ma.account_id)
        attach_imported_cookie(ma)
        if ma.login_via_cookie:
            attached += 1
    return attached


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


def parse_account_line(
    line: str,
    *,
    default_browser: str = "firefox",
    account_format: str = "mail",
) -> MappedAccountAuth:
    """
    Parse một dòng tài khoản → ``MappedAccountAuth``.

    ``account_format``:
    - ``mail``: uid, pass, 2fa, mail, pass_mail, mail_khoi_phuc
    - ``cookie``: UID, mật khẩu, 2FA, cookie, mail khôi phục, mật khẩu mail
    - ``auto``: nhận theo từng dòng
    """
    _ = default_browser
    fmt = normalize_account_line_format(account_format)
    if fmt == "auto":
        fmt = detect_account_line_format(_split_delimited(line))
    if fmt == "cookie":
        auth = _parse_cookie_account_parts(_split_delimited(line))
        if not auth.username:
            raise ValueError("Thiếu UID/username.")
        return auth
    username, password, totp, email, email_pass, recovery = split_account_fields(line)
    if not username:
        raise ValueError("Thiếu UID/username.")

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
    Khóa slot proxy.

    Không có user: ``host:port`` (một IP trần = một tài khoản).
    Có user/pass: ``host:port|user|pass`` — gateway SOCKS (cùng host, session khác nhau)
    là các proxy khác nhau, để «Cập nhật proxy» nhận dòng mới vừa dán.
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
    user = str(net.proxy_username or px.get("user") or "").strip().lower()
    password = str(net.proxy_password or px.get("pass") or "").strip()
    if host and port > 0:
        if user or password:
            return f"{host}:{port}|{user}|{password}"
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


def _mapped_matches_post_account(mapped: MappedAccount, acc: dict[str, Any]) -> bool:
    """Khớp dòng tab Đăng nhập / Tương tác với bản ghi dùng để đăng job."""
    aid = str(acc.get("id") or "").strip()
    uid = str(acc.get("facebook_uid") or "").strip()
    email = str(acc.get("email") or "").strip().lower()
    if aid and mapped.account_id == aid:
        return True
    disp = mapped.display_uid()
    if uid and (disp == uid or mapped.account_id in (uid, f"UID_{uid}")):
        return True
    mapped_uid = _extract_facebook_uid(mapped.account_id, username=mapped.auth.username)
    if uid and mapped_uid.isdigit() and mapped_uid == uid:
        return True
    mapped_email = str(mapped.auth.email or "").strip().lower()
    return bool(email and mapped_email and email == mapped_email)


def _pick_login_page_account_for_post(
    acc: dict[str, Any],
    settings: dict[str, Any],
) -> MappedAccount | None:
    """
    Chọn bản ghi mới nhất trên tab đăng nhập để đăng job.

    Ưu tiên hàng tương tác đã ``login_ok`` / ``success``, sau đó các dòng khớp còn lại.
    """
    from src.utils.human_interaction_settings import (
        load_interaction_queue_from_settings,
        load_login_queue_from_settings,
    )

    ranked: list[tuple[int, int, MappedAccount]] = []
    queues = (
        (2, load_interaction_queue_from_settings(settings)),
        (0, load_login_queue_from_settings(settings)),
    )
    for queue_bonus, rows in queues:
        for index, raw in enumerate(rows):
            try:
                mapped = MappedAccount.from_dict(raw)
            except Exception as exc:  # noqa: BLE001
                logger.debug("[Post] Bỏ dòng tab đăng nhập hỏng: {}", exc)
                continue
            if not _mapped_matches_post_account(mapped, acc):
                continue
            status_bonus = 4 if mapped.status in ("login_ok", "success") else 0
            ranked.append((queue_bonus + status_bonus, index, mapped))
    if not ranked:
        return None
    ranked.sort(key=lambda item: (item[0], item[1]))
    return ranked[-1][2]


def _sync_login_secrets_onto_registry(mapped: MappedAccount, registry_id: str) -> None:
    """Ghi mật khẩu/2FA mới từ tab đăng nhập vào vault của id dùng khi đăng job."""
    apply_mapped_secrets_to_vault(mapped)
    rid = str(registry_id or "").strip()
    if not rid or rid == str(mapped.account_id or "").strip():
        return
    from src.utils.account_credentials import set_account_credentials

    kwargs: dict[str, Any] = {}
    if mapped.auth.password:
        kwargs["password"] = mapped.auth.password
    if mapped.auth.two_fa_secret:
        kwargs["totp_secret"] = mapped.auth.two_fa_secret
    if mapped.auth.recovery_email:
        kwargs["recovery_email"] = mapped.auth.recovery_email
    if kwargs:
        set_account_credentials(rid, **kwargs)


def refresh_post_account_from_login_page(
    acc: dict[str, Any],
    *,
    settings: dict[str, Any] | None = None,
    persist: bool = True,
) -> dict[str, Any]:
    """
    Đưa proxy, cookie, profile và mật khẩu mới nhất từ tab Đăng nhập vào account trước khi đăng.

    ``accounts.json`` có thể còn proxy/phiên cũ. Job đăng đọc dict này nên phải phủ bằng
    dòng đã ghép (ưu tiên tab Tương tác đã đăng nhập thành công) rồi lưu lại registry.
    """
    from src.utils.human_interaction_settings import load_human_interaction_settings
    from src.utils.proxy_check import proxy_host_port_configured

    try:
        loaded = settings if settings is not None else load_human_interaction_settings()
    except Exception as exc:  # noqa: BLE001
        logger.warning("[Post] Không đọc được tab đăng nhập — đăng bằng accounts.json: {}", exc)
        return acc
    if not isinstance(loaded, dict) or not loaded:
        return acc
    mapped = _pick_login_page_account_for_post(acc, loaded)
    if mapped is None:
        return acc

    updates: dict[str, Any] = {}
    if mapped.use_proxy:
        px = network_to_proxy_config(mapped.network)
        if proxy_host_port_configured(px):  # type: ignore[arg-type]
            acc["proxy"] = px
            acc["use_proxy"] = True
            updates["proxy"] = px
            updates["use_proxy"] = True
    cookie = str(mapped.cookie_path or "").strip()
    if cookie and Path(cookie).is_file():
        acc["cookie_path"] = cookie
        updates["cookie_path"] = cookie
    profile = str(mapped.storage.profile_path or "").strip()
    if profile and Path(profile).is_dir():
        acc["portable_path"] = profile
        acc["profile_path"] = profile
        updates["portable_path"] = profile
        updates["profile_path"] = profile
    email = str(mapped.auth.email or "").strip()
    if email:
        acc["email"] = email
        updates["email"] = email
    recovery = str(mapped.auth.recovery_email or "").strip()
    if recovery:
        acc["recovery_email"] = recovery
        updates["recovery_email"] = recovery
    if mapped.auth.two_fa_secret:
        acc["totp_enabled"] = True
        updates["totp_enabled"] = True
    uid = mapped.display_uid()
    if uid.isdigit() and not str(acc.get("facebook_uid") or "").strip().isdigit():
        acc["facebook_uid"] = uid
        updates["facebook_uid"] = uid

    registry_id = str(acc.get("id") or "").strip()
    try:
        _sync_login_secrets_onto_registry(mapped, registry_id)
    except Exception as exc:  # noqa: BLE001
        logger.warning("[Post] Không ghi được mật khẩu mới vào vault {}: {}", registry_id, exc)

    if persist and updates and registry_id:
        try:
            from src.utils.db_manager import AccountsDatabaseManager

            AccountsDatabaseManager().update_account_fields(registry_id, updates)
            logger.info(
                "[Post] Đã cập nhật accounts.json từ tab đăng nhập account={} fields={}",
                registry_id,
                ",".join(sorted(updates)),
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "[Post] Dùng dữ liệu tab đăng nhập cho lần đăng này, chưa ghi accounts.json {}: {}",
                registry_id,
                exc,
            )
    elif updates:
        logger.info(
            "[Post] Áp dữ liệu tab đăng nhập cho account={} fields={}",
            registry_id or mapped.account_id,
            ",".join(sorted(updates)),
        )
    return acc


def prepare_account_dict_for_browser_run(
    acc: dict[str, Any],
    *,
    require_proxy_live: bool = True,
) -> dict[str, Any]:
    """
    Chuẩn bị account dict end-to-end trước ``launch_persistent_context`` (đăng lịch, đăng ngay).

    Registry → dữ liệu mới nhất từ tab Đăng nhập → profile → cookie → kiểm tra proxy LIVE.
    """
    from src.utils.account_browser_profile import ensure_account_browser_profile_ready

    base = dict(acc)
    enrich_account_dict_from_registry(base)
    refresh_post_account_from_login_page(base)
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


def enrich_account_dict_from_registry(
    acc: dict[str, Any],
    *,
    registry_rows: list[dict[str, Any]] | None = None,
) -> None:
    """
    Gắn profile portable, cookie, trình duyệt từ ``accounts.json`` nếu ``id`` đã tồn tại.

    Giữ proxy trong ``acc`` (từ dòng ghép) — chỉ đồng bộ phần lưu trữ phiên giống job đăng bài.
    """
    aid = str(acc.get("id") or "").strip()
    if not aid:
        return
    try:
        if registry_rows is not None:
            rows = registry_rows
        else:
            from src.utils.db_manager import AccountsDatabaseManager

            rows = AccountsDatabaseManager().load_all()
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

    Đọc ``accounts.json`` một lần cho cả danh sách — không mở file lại từng tài khoản
    (gọi trên luồng nền, không trên luồng giao diện).
    """
    rows: list[dict[str, Any]] | None = None
    try:
        from src.utils.db_manager import AccountsDatabaseManager

        loaded = AccountsDatabaseManager().load_all()
        rows = [dict(r) for r in loaded]
    except Exception as exc:  # noqa: BLE001
        logger.debug("[Human] refresh storage không đọc được accounts.json: {}", exc)
    for ma in mapped_list:
        try:
            sync_mapped_account_storage_from_registry(ma, registry_rows=rows)
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


def sync_mapped_account_storage_from_registry(
    mapped: MappedAccount,
    *,
    registry_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """
    Đồng bộ ``profile_path`` / ``cookie_path`` từ ``accounts.json`` (theo id hoặc ``facebook_uid``).

    Giữ ``mapped.account_id`` (UID hiển thị GUI) — chỉ cập nhật đường dẫn lưu trữ phiên thực tế.

    ``registry_rows`` nếu đã đọc sẵn thì không mở ``accounts.json`` lại.
    """
    acc = mapped_account_to_account_dict(mapped, registry_rows=registry_rows)
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


def mapped_account_to_account_dict(
    mapped: MappedAccount,
    *,
    registry_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
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
    enrich_account_dict_from_registry(out, registry_rows=registry_rows)
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


_PROFILE_DIE_MARKERS: tuple[str, ...] = (
    "this content isn't available",
    "this content isn’t available",
    "content isn't available right now",
    "this page isn't available",
    "the page you requested cannot be displayed",
    "sorry, this content isn't available",
    "the link you followed may be broken",
    "page may have been removed",
    "account has been disabled",
    "nội dung này hiện không có",
    "nội dung này hiện không khả dụng",
    "trang này không hiển thị",
    "trang này hiện không khả dụng",
    "tài khoản này đã bị vô hiệu hóa",
    "tài khoản đã bị vô hiệu hóa",
    "bạn hiện không xem được nội dung này",
    "không xem được nội dung này",
    "chỉ chia sẻ nội dung với một nhóm nhỏ",
    "lỗi này thường do chủ sở hữu",
    "đi đến bảng feed",
    "đã xóa nội dung",
    "you can't see this content",
    "you can’t see this content",
    "the owner only shared it with a small group",
)

_PROFILE_TITLE_SKIP: frozenset[str] = frozenset(
    {
        "",
        "facebook",
        "log in",
        "log into facebook",
        "login",
        "đăng nhập",
        "error",
        "lỗi",
        "something went wrong",
        "page not found",
    }
)


def extract_uid_lines(text: str) -> list[str]:
    """Mỗi dòng lấy một UID số để copy. Ưu tiên ``c_user``, không có thì lấy dãy số dài nhất."""
    found: list[str] = []
    seen: set[str] = set()
    for line in str(text or "").splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#"):
            continue
        uid = facebook_uid_in_text(raw)
        if not uid:
            runs = re.findall(r"\d{6,20}", raw)
            uid = max(runs, key=len) if runs else ""
        if uid and uid not in seen:
            seen.add(uid)
            found.append(uid)
    return found


def facebook_uid_in_text(text: str) -> str:
    """Lấy UID từ ``c_user=1000…`` hoặc JSON cookie. Không thấy thì chuỗi rỗng."""
    raw = str(text or "")
    match = re.search(
        r"""c_user(?:\\?["']|%22)?\s*[:=]\s*(?:\\?["'])?(\d{6,20})""",
        raw,
        flags=re.I,
    )
    if match:
        return match.group(1)
    match = re.search(
        r"""c_user["']\s*,\s*["']value["']\s*:\s*["'](\d{6,20})""",
        raw,
        flags=re.I,
    )
    if match:
        return match.group(1)
    return ""


def live_result_rank(label: str) -> int:
    """Thứ tự bảng Check Live: Live trên cùng, Die dưới cùng."""
    if label == "Live":
        return 0
    if label in {"Đang check", "Đang chờ"}:
        return 1
    if label in {"Lỗi", "Lỗi proxy"}:
        return 2
    if label in {"Die", "Checkpoint"}:
        return 3
    return 4


def facebook_uid_for_live_check(mapped: MappedAccount) -> str:
    """UID số để xem hồ sơ công khai. Mail/pass không dùng — check live không đăng nhập."""
    cookie = str(getattr(mapped.auth, "imported_cookie", "") or "")
    found = facebook_uid_in_text(cookie)
    if found:
        return found
    for raw in (mapped.auth.username, mapped.account_id, mapped.display_uid()):
        token = str(raw or "").strip()
        if token.upper().startswith("UID_"):
            token = token[4:]
        if token.isdigit() and 6 <= len(token) <= 20:
            return token
        found = facebook_uid_in_text(token)
        if found:
            return found
    return ""


def _decode_profile_text(raw: str) -> str:
    """Giải mã HTML entity và \\uXXXX để câu khóa hồ sơ khớp với chữ trên màn hình."""
    text = html_module.unescape(raw or "")

    def _unichar(match: re.Match[str]) -> str:
        try:
            return chr(int(match.group(1), 16))
        except ValueError:
            return match.group(0)

    text = re.sub(r"\\u([0-9a-fA-F]{4})", _unichar, text)
    return text


def _visible_profile_text(html: str) -> str:
    """Bỏ script và style. Chỉ còn chữ người dùng nhìn thấy trên hồ sơ."""
    cleaned = re.sub(r"<script\b[^>]*>.*?</script>", " ", html or "", flags=re.I | re.S)
    cleaned = re.sub(r"<style\b[^>]*>.*?</style>", " ", cleaned, flags=re.I | re.S)
    cleaned = re.sub(r"<[^>]+>", " ", cleaned)
    return " ".join(_decode_profile_text(cleaned).split()).casefold()


def _profile_title(html: str) -> str:
    match = re.search(r"<title>(.*?)</title>", html or "", flags=re.I | re.S)
    title = re.sub(r"\s+", " ", _decode_profile_text(match.group(1))).strip() if match else ""
    return re.sub(r"\s*[|\-]\s*facebook\s*$", "", title, flags=re.I).strip()


def _profile_meta_name(html: str) -> str:
    """Tên hồ sơ trong thẻ og:title. Trang lỗi thường không có tên người."""
    patterns = (
        r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)',
        r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:title["\']',
    )
    for pattern in patterns:
        match = re.search(pattern, html or "", flags=re.I)
        if match:
            return " ".join(_decode_profile_text(match.group(1)).split())
    return ""


def _profile_name_is_usable(name: str) -> bool:
    raw = " ".join((name or "").split())
    if len(raw) < 2 or _profile_title_is_error(raw):
        return False
    low = raw.casefold()
    if low in _PROFILE_TITLE_SKIP or "log in" in low or "đăng nhập" in low:
        return False
    if any(token in low for token in ("something went wrong", "đã xảy ra lỗi", "sorry, something")):
        return False
    return True


def _profile_shows_live_account(visible: str) -> bool:
    """Hồ sơ còn mở: có người theo dõi hoặc tab Bài viết và Giới thiệu."""
    if any(token in visible for token in ("người theo dõi", "đang theo dõi", "followers", "people follow this")):
        return True
    return "bài viết" in visible and "giới thiệu" in visible


def classify_public_profile_html(html: str) -> tuple[str, str]:
    """
    Đọc HTML hồ sơ công khai (không phiên đăng nhập).

    Returns:
        ``(status, detail)`` — ``login_ok`` = còn hoạt động, ``login_failed`` = die/checkpoint.
    """
    visible = _visible_profile_text(html)
    title = _profile_title(html)
    meta_name = _profile_meta_name(html)
    if any(marker in visible for marker in _PROFILE_DIE_MARKERS):
        return "login_failed", "UID không còn hoạt động"
    if "checkpoint" in visible and ("confirm your identity" in visible or "xác minh danh tính" in visible):
        return "login_failed", "Checkpoint — tài khoản bị khóa xác minh"
    if any(
        marker in visible
        for marker in (
            "trình duyệt này không hỗ trợ",
            "this browser is not supported",
            "đăng nhập hoặc đăng ký để xem",
            "log in or sign up to view",
            "sorry, something went wrong",
            "something went wrong",
            "xin lỗi, đã xảy ra lỗi",
            "đã xảy ra lỗi",
        )
    ):
        return "error", "Facebook báo lỗi — chưa biết UID còn hoạt động hay không"
    if _profile_shows_live_account(visible):
        name = meta_name or title or "hồ sơ công khai"
        if not _profile_name_is_usable(name):
            name = "hồ sơ công khai"
        return "login_ok", f"Còn hoạt động — {name[:80]}"
    for candidate in (meta_name, title):
        if _profile_name_is_usable(candidate):
            return "login_ok", f"Còn hoạt động — {candidate[:80]}"
    if _profile_title_is_error(title) or (meta_name and _profile_title_is_error(meta_name)):
        return "error", "Facebook báo lỗi — chưa biết UID còn hoạt động hay không"
    return "error", "Không xác định được — Facebook không trả tên hồ sơ công khai"


def _profile_title_is_error(title: str) -> bool:
    """«Error Facebook» là trang lỗi, không phải tên tài khoản."""
    raw = " ".join((title or "").split()).casefold()
    raw = re.sub(r"\s*[|\-]\s*facebook\s*$", "", raw).strip()
    if raw in _PROFILE_TITLE_SKIP or raw in {"error facebook", "facebook error"}:
        return True
    tokens = set(re.findall(r"[a-z]+", raw))
    return bool(tokens) and tokens <= {"error", "facebook", "something", "went", "wrong", "sorry"}


def requests_proxy_url_for_network(net: MappedAccountNetwork) -> str:
    """URL proxy cho ``requests`` (kèm user/pass nếu có). Rỗng nếu không cấu hình proxy."""
    server = str(net.proxy_server or "").strip()
    if not server:
        return ""
    if "://" not in server:
        server = f"http://{server}"
    parsed = urlparse(server)
    user = str(net.proxy_username or "").strip()
    password = str(net.proxy_password or "")
    if user and not parsed.username and parsed.hostname:
        from urllib.parse import quote

        host = parsed.hostname
        port = f":{parsed.port}" if parsed.port else ""
        netloc = f"{quote(user, safe='')}:{quote(password, safe='')}@{host}{port}"
        return f"{parsed.scheme}://{netloc}"
    return server


def probe_facebook_uid_live(
    uid: str,
    *,
    proxy_url: str | None = None,
    timeout: float = 15.0,
) -> tuple[str, str]:
    """
    Xem UID còn hoạt động qua trang hồ sơ. Không gửi mật khẩu, không gọi Graph API.

    ``mbasic`` thường trả trang lỗi. Thử Facebook thường trước, chỉ kết luận Lỗi khi mọi trang đều không rõ.
    """
    import requests

    target = str(uid or "").strip()
    if not target.isdigit():
        return "error", "Thiếu UID số — check live không đăng nhập"
    urls = (
        f"https://www.facebook.com/profile.php?id={target}",
        f"https://m.facebook.com/profile.php?id={target}",
        f"https://mbasic.facebook.com/profile.php?id={target}",
    )
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept": "text/html,application/xhtml+xml",
    }
    proxies = None
    if str(proxy_url or "").strip():
        proxies = {"http": proxy_url, "https": proxy_url}
    last = ("error", "Facebook báo lỗi — chưa biết UID còn hoạt động hay không")
    missing = False
    for url in urls:
        try:
            response = requests.get(
                url,
                headers=headers,
                timeout=timeout,
                allow_redirects=True,
                proxies=proxies,
            )
        except requests.RequestException as exc:
            msg = str(exc).lower()
            if proxies and any(k in msg for k in ("proxy", "socks", "tunnel", "timed out", "timeout", "407")):
                return "proxy_error", f"Proxy lỗi: {exc}"[:180]
            last = ("error", f"Không kết nối được Facebook: {exc}"[:180])
            continue
        if response.status_code in (404, 410):
            missing = True
            continue
        status, detail = classify_public_profile_html(response.text or "")
        if status in {"login_ok", "login_failed"}:
            return status, detail
        last = (status, detail)
    if missing and last[0] == "error":
        return "login_failed", "UID không còn hoạt động"
    return last


def classify_account_live(status: str, detail: str = "") -> str:
    """Nhãn kết quả check live: Live / Die / Checkpoint / Lỗi proxy / Đã hủy / Lỗi."""
    st = str(status or "").strip().lower()
    d = str(detail or "").lower()
    if st in {"login_ok", "success"}:
        return "Live"
    if st in {"pending", "waiting"}:
        return "Đang chờ"
    if st == "running":
        return "Đang check"
    if any(k in d for k in ("checkpoint", "xác minh danh tính", "xac minh danh tinh", "captcha")):
        return "Checkpoint"
    if st == "proxy_error":
        return "Lỗi proxy"
    if st == "login_failed":
        return "Die"
    if st == "cancelled":
        return "Đã hủy"
    return "Lỗi"


def map_pasted_accounts_for_live_check(
    account_lines: list[str],
    proxy_lines: list[str] | None = None,
    *,
    browser_type: str = "firefox",
    account_format: str = "mail",
) -> list[MappedAccount]:
    """
    Ghép list dán để check live — profile/cookie nằm trong ``data/runtime/live_check``.

    Không ghi vault, không chặn proxy đã gắn ``accounts.json`` (phiên kiểm tra tách biệt).
    """
    from src.utils.paths import project_root

    acc = [ln.strip() for ln in account_lines if str(ln or "").strip() and not str(ln).strip().startswith("#")]
    px = [ln.strip() for ln in (proxy_lines or []) if str(ln or "").strip() and not str(ln).strip().startswith("#")]
    if not acc:
        raise AccountProxyMappingError("Danh sách tài khoản trống — dán uid|pass|2fa|mail|…")
    if px and len(px) < len(acc):
        raise AccountProxyMappingError(
            f"Số proxy ({len(px)}) ít hơn số tài khoản ({len(acc)}). "
            "Dán đủ proxy 1:1 hoặc để trống ô proxy (check không proxy)."
        )
    root = project_root() / "data" / "runtime" / "live_check"
    bt = normalize_browser_storage(browser_type)
    out: list[MappedAccount] = []
    seen_proxy: dict[str, str] = {}
    for i, line in enumerate(acc):
        try:
            auth = parse_account_line(
                line, default_browser=browser_type, account_format=account_format
            )
        except ValueError as exc:
            raise AccountProxyMappingError(f"Dòng tài khoản {i + 1}: {exc}") from exc
        found_uid = facebook_uid_in_text(line) or facebook_uid_in_text(auth.imported_cookie)
        if found_uid:
            if _looks_like_email(auth.username):
                auth.email = auth.username
            auth.username = found_uid
        aid = _account_id_from_auth(auth, i)
        network = MappedAccountNetwork()
        use_proxy = False
        if px:
            try:
                network = parse_proxy_line_to_network(px[i])
            except ValueError as exc:
                raise AccountProxyMappingError(f"Dòng proxy {i + 1}: {exc}") from exc
            use_proxy = True
            pkey = proxy_identity_key_for_network(network)
            if pkey and pkey in seen_proxy:
                raise AccountProxyMappingError(
                    format_proxy_exclusive_error(
                        pkey,
                        seen_proxy[pkey],
                        offender_id=aid,
                        context=f"dòng proxy {i + 1} trùng dòng trước",
                    )
                )
            if pkey:
                seen_proxy[pkey] = aid
        prof = root / "profiles" / aid
        cookie = root / "cookies" / f"{aid}.json"
        out.append(
            MappedAccount(
                account_id=aid,
                auth=auth,
                network=network,
                storage=MappedAccountStorage(profile_path=str(prof)),
                browser_type=bt,
                cookie_path=str(cookie),
                use_proxy=use_proxy,
                status="pending",
                grid_slot_index=i,
            )
        )
    return out


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


def _proxy_blocked_for_candidate(
    pkey: str,
    candidate_id: str,
    *,
    username: str,
    seen_proxy_keys: dict[str, str],
    registry_index: dict[str, str],
) -> str:
    """
    Trả về id chủ proxy nếu proxy này không được gắn cho ứng viên.

    Chuỗi rỗng nghĩa là proxy còn dùng được (trống, hoặc chính tài khoản này đang giữ).
    """
    if not pkey:
        return ""
    aliases = _account_alias_ids(
        candidate_id,
        facebook_uid=_extract_facebook_uid(candidate_id, username=username),
    )
    seen_owner = seen_proxy_keys.get(pkey, "")
    if seen_owner and seen_owner not in aliases:
        return seen_owner
    reg_owner = registry_index.get(pkey, "")
    if reg_owner and reg_owner not in aliases:
        return reg_owner
    return ""


def map_accounts_with_proxies(
    account_lines: list[str],
    proxy_lines: list[str],
    *,
    max_concurrent: int,
    browser_type: str = "firefox",
    persist_secrets: bool = True,
    account_format: str = "mail",
    extra_blocked: dict[str, str] | None = None,
) -> list[MappedAccount]:
    """
    Ghép mỗi tài khoản với một proxy còn trống trong list.

    Proxy trùng dòng khác, đã gắn ``accounts.json``, hoặc nằm trong ``extra_blocked``
    (hàng đợi đang mở) thì bỏ qua, lấy proxy kế tiếp còn dùng được.

    Raises:
        AccountProxyMappingError: Thiếu proxy trống cho một tài khoản.
    """
    n_acc = len(account_lines)
    n_px = len(proxy_lines)
    mc = max(1, int(max_concurrent))

    if n_acc == 0:
        raise AccountProxyMappingError("Danh sách tài khoản trống.")
    if n_px == 0:
        raise AccountProxyMappingError("Danh sách proxy trống.")

    parsed_proxies: list[tuple[int, MappedAccountNetwork, str]] = []
    for j, line in enumerate(proxy_lines):
        try:
            network = parse_proxy_line_to_network(line)
        except ValueError as exc:
            raise AccountProxyMappingError(f"Dòng proxy {j + 1}: {exc}") from exc
        parsed_proxies.append((j, network, proxy_identity_key_for_network(network)))

    mapped_list: list[MappedAccount] = []
    seen_ids: dict[str, int] = {}
    seen_proxy_keys: dict[str, str] = {}
    used_proxy_rows: set[int] = set()
    registry_index = load_registry_proxy_index()
    for key, owner in (extra_blocked or {}).items():
        if key and owner:
            registry_index.setdefault(str(key), str(owner))

    for i in range(n_acc):
        try:
            auth = parse_account_line(
                account_lines[i],
                default_browser=browser_type,
                account_format=account_format,
            )
        except ValueError as exc:
            raise AccountProxyMappingError(f"Dòng tài khoản {i + 1}: {exc}") from exc

        base_aid = _account_id_from_auth(auth, i)
        pick_order = [i] + [j for j in range(n_px) if j != i]
        chosen: tuple[int, MappedAccountNetwork, str] | None = None
        for j in pick_order:
            if j < 0 or j >= n_px or j in used_proxy_rows:
                continue
            row_no, network, pkey = parsed_proxies[j]
            blocker = _proxy_blocked_for_candidate(
                pkey,
                base_aid,
                username=auth.username,
                seen_proxy_keys=seen_proxy_keys,
                registry_index=registry_index,
            )
            if blocker:
                logger.info(
                    "[Human/Proxy] Bỏ proxy dòng {} ({}) — đã gắn «{}», tìm proxy khác.",
                    row_no + 1,
                    pkey,
                    blocker,
                )
                continue
            chosen = (row_no, network, pkey)
            break
        if chosen is None:
            logger.warning(
                "[Human/Proxy] Dòng tài khoản {} không còn proxy trống — bỏ qua.",
                i + 1,
            )
            continue
        row_no, network, pkey = chosen
        used_proxy_rows.add(row_no)
        if row_no != i:
            logger.info(
                "[Human/Proxy] Tài khoản dòng {} dùng proxy dòng {} (dòng {} đã bị trùng).",
                i + 1,
                row_no + 1,
                i + 1,
            )
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
        attach_imported_cookie(ma)
        if row_no != i and "cookie" not in (ma.status_detail or "").lower():
            ma.status_detail = f"Đổi sang proxy dòng {row_no + 1} (proxy trước đã gắn TK khác)"
        try:
            sync_mapped_account_storage_from_registry(ma)
        except Exception as sync_exc:  # noqa: BLE001
            logger.warning("[Human] Sync registry sau ghép dòng {}: {}", aid, sync_exc)
        if pkey:
            seen_proxy_keys[pkey] = aid
        mapped_list.append(ma)

    if not mapped_list:
        raise AccountProxyMappingError(
            "Không còn proxy trống trong list. Proxy trùng hoặc đã gắn tài khoản khác đã bị bỏ qua — thêm proxy mới rồi ghép lại."
        )

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
    # Dòng chưa gắn TK (proxy mới dán thêm) được thử trước dòng đã dùng.
    fresh: list[str] = []
    taken: list[str] = []
    for raw_line in pool:
        try:
            pkey = proxy_identity_key_for_network(parse_proxy_line_to_network(raw_line))
        except ValueError:
            taken.append(raw_line)
            continue
        if pkey and pkey not in used_keys:
            fresh.append(raw_line)
        else:
            taken.append(raw_line)
    pool = fresh + taken

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
    account_format: str = "mail",
) -> list[MappedAccount]:
    """Đọc hai file và ghép."""
    return map_accounts_with_proxies(
        read_lines_file(accounts_path),
        read_lines_file(proxies_path),
        max_concurrent=max_concurrent,
        browser_type=browser_type,
        account_format=account_format,
    )
