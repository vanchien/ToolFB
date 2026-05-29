"""
Lưu mật khẩu / TOTP secret tài khoản Facebook — tách khỏi ``accounts.json``.

File ``config/account_credentials.json`` (gitignored). Chỉ tham chiếu qua ``password_ref`` / ``totp_secret_ref`` trên bản ghi account.
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from loguru import logger

from src.utils.paths import project_root

_CREDENTIALS_NAME = "account_credentials.json"
_store_cache: dict[str, Any] | None = None
_store_mtime: float | None = None


def account_credentials_path() -> Path:
    return project_root() / "config" / _CREDENTIALS_NAME


def _atomic_write_json(path: Path, data: dict[str, Any]) -> None:
    global _store_cache, _store_mtime
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(data, ensure_ascii=False, indent=2) + "\n"
    fd, tmp = tempfile.mkstemp(prefix="account_credentials_", suffix=".tmp.json", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(text)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
        _store_cache = data
        _store_mtime = path.stat().st_mtime
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def load_account_credentials_store(*, force_reload: bool = False) -> dict[str, Any]:
    global _store_cache, _store_mtime
    p = account_credentials_path()
    if not force_reload and _store_cache is not None and p.is_file():
        try:
            mt = p.stat().st_mtime
            if _store_mtime is not None and mt == _store_mtime:
                return _store_cache
        except OSError:
            pass
    if not p.is_file():
        _store_cache = {"accounts": {}}
        _store_mtime = None
        return _store_cache
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không đọc account_credentials.json: {}", exc)
        raw = {"accounts": {}}
    if not isinstance(raw, dict):
        raw = {"accounts": {}}
    if not isinstance(raw.get("accounts"), dict):
        raw["accounts"] = {}
    _store_cache = raw
    try:
        _store_mtime = p.stat().st_mtime
    except OSError:
        _store_mtime = None
    return raw


def _invalidate_credentials_cache() -> None:
    global _store_cache, _store_mtime
    _store_cache = None
    _store_mtime = None


def _resolve_account_key(account_id: str, ref: str | None) -> str:
    r = str(ref or "").strip()
    if r.startswith("account:"):
        return r.split(":", 1)[1].strip()
    if r:
        return r
    return str(account_id or "").strip()


def get_account_password(account_id: str, password_ref: str | None = None) -> str:
    aid = _resolve_account_key(account_id, password_ref)
    if not aid:
        return ""
    store = load_account_credentials_store()
    row = (store.get("accounts") or {}).get(aid)
    if not isinstance(row, dict):
        return ""
    return str(row.get("password") or "").strip()


def get_account_totp_secret(account_id: str, totp_secret_ref: str | None = None) -> str:
    aid = _resolve_account_key(account_id, totp_secret_ref)
    if not aid:
        return ""
    store = load_account_credentials_store()
    row = (store.get("accounts") or {}).get(aid)
    if not isinstance(row, dict):
        return ""
    return str(row.get("totp_secret") or "").strip()


@dataclass(frozen=True)
class AccountCredentialBundle:
    account_id: str
    email: str
    password: str
    totp_secret: str
    totp_enabled: bool

    @property
    def has_password_login(self) -> bool:
        return bool(self.email and self.password)

    @property
    def has_totp(self) -> bool:
        return bool(self.totp_enabled and self.totp_secret)


def load_account_credential_bundle(account: dict[str, Any] | None) -> AccountCredentialBundle | None:
    if not account:
        return None
    aid = str(account.get("id", "")).strip()
    if not aid:
        return None
    from src.services.totp_service import normalize_totp_secret

    email = str(account.get("email") or "").strip()
    password = get_account_password(aid, str(account.get("password_ref") or "") or None)
    totp_raw = ""
    if bool(account.get("totp_enabled")):
        totp_raw = normalize_totp_secret(
            get_account_totp_secret(aid, str(account.get("totp_secret_ref") or "") or None)
        )
    return AccountCredentialBundle(
        account_id=aid,
        email=email,
        password=password,
        totp_secret=totp_raw,
        totp_enabled=bool(account.get("totp_enabled")),
    )


def set_account_credentials(
    account_id: str,
    *,
    password: str | None = None,
    totp_secret: str | None = None,
    clear_password: bool = False,
    clear_totp: bool = False,
) -> None:
    """Ghi hoặc xóa secret cho ``account_id``."""
    aid = str(account_id or "").strip()
    if not aid:
        raise ValueError("account_id rỗng.")
    store = load_account_credentials_store(force_reload=True)
    accounts = store.setdefault("accounts", {})
    if not isinstance(accounts, dict):
        accounts = {}
        store["accounts"] = accounts
    row: dict[str, str] = dict(accounts.get(aid) or {})
    if clear_password:
        row.pop("password", None)
    elif password is not None:
        row["password"] = str(password)
    if clear_totp:
        row.pop("totp_secret", None)
    elif totp_secret is not None:
        row["totp_secret"] = str(totp_secret).strip().replace(" ", "")
    if row:
        accounts[aid] = row
    else:
        accounts.pop(aid, None)
    _atomic_write_json(account_credentials_path(), store)


def account_can_auto_reauth(account: dict[str, Any] | None) -> bool:
    """Email + password trong vault — TOTP là bước tùy chọn sau login."""
    bundle = load_account_credential_bundle(account)
    return bool(bundle and bundle.has_password_login)


def account_has_totp_recovery(account: dict[str, Any] | None) -> bool:
    """Tương thích cũ: đủ email + password + TOTP secret."""
    bundle = load_account_credential_bundle(account)
    return bool(bundle and bundle.has_password_login and bundle.has_totp)


def default_password_ref(account_id: str) -> str:
    return f"account:{account_id}"


def default_totp_secret_ref(account_id: str) -> str:
    return f"account:{account_id}"
