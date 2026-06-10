"""
Object tài khoản sau khi gộp account + proxy (Mapped Account Object).

Dùng cho hàng đợi tương tác giống người dùng — tách ``auth`` / ``network`` / ``storage``.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class MappedAccountAuth:
    """Thông tin đăng nhập (có thể đọc từ file hoặc vault)."""

    username: str = ""
    password: str = ""
    two_fa_secret: str = ""
    email: str = ""
    email_password: str = ""
    recovery_email: str = ""

    def to_dict(self) -> dict[str, str]:
        return {
            "username": self.username,
            "password": self.password,
            "two_fa_secret": self.two_fa_secret,
            "email": self.email,
            "email_password": self.email_password,
            "recovery_email": self.recovery_email,
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any] | None) -> MappedAccountAuth:
        if not isinstance(raw, dict):
            return cls()
        return cls(
            username=str(raw.get("username") or "").strip(),
            password=str(raw.get("password") or "").strip(),
            two_fa_secret=str(raw.get("two_fa_secret") or raw.get("totp_secret") or "").strip(),
            email=str(raw.get("email") or "").strip(),
            email_password=str(raw.get("email_password") or "").strip(),
            recovery_email=str(raw.get("recovery_email") or "").strip(),
        )


@dataclass
class MappedAccountNetwork:
    """Proxy gán 1:1 cho luồng."""

    proxy_server: str = ""
    proxy_username: str = ""
    proxy_password: str = ""

    def to_dict(self) -> dict[str, str]:
        return {
            "proxy_server": self.proxy_server,
            "proxy_username": self.proxy_username,
            "proxy_password": self.proxy_password,
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any] | None) -> MappedAccountNetwork:
        if not isinstance(raw, dict):
            return cls()
        return cls(
            proxy_server=str(raw.get("proxy_server") or "").strip(),
            proxy_username=str(raw.get("proxy_username") or raw.get("user") or "").strip(),
            proxy_password=str(raw.get("proxy_password") or raw.get("pass") or "").strip(),
        )


@dataclass
class MappedAccountStorage:
    """Profile Playwright persistent."""

    profile_path: str = ""

    def to_dict(self) -> dict[str, str]:
        return {"profile_path": self.profile_path}

    @classmethod
    def from_dict(cls, raw: dict[str, Any] | None) -> MappedAccountStorage:
        if not isinstance(raw, dict):
            return cls()
        return cls(profile_path=str(raw.get("profile_path") or "").strip())


@dataclass
class MappedAccount:
    """Bản ghi đầy đủ đưa vào worker queue."""

    account_id: str
    auth: MappedAccountAuth = field(default_factory=MappedAccountAuth)
    network: MappedAccountNetwork = field(default_factory=MappedAccountNetwork)
    storage: MappedAccountStorage = field(default_factory=MappedAccountStorage)
    browser_type: str = "firefox"
    cookie_path: str = ""
    use_proxy: bool = True
    status: str = "pending"
    status_detail: str = ""
    grid_slot_index: int = 0
    # True = tab Tương tác: thử profile/cookie trước; chỉ form login khi mở browser mà chưa có phiên.
    soft_login_if_needed: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "account_id": self.account_id,
            "auth": self.auth.to_dict(),
            "network": self.network.to_dict(),
            "storage": self.storage.to_dict(),
            "browser_type": self.browser_type,
            "cookie_path": self.cookie_path,
            "use_proxy": self.use_proxy,
            "status": self.status,
            "status_detail": self.status_detail,
            "grid_slot_index": self.grid_slot_index,
            "soft_login_if_needed": self.soft_login_if_needed,
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> MappedAccount:
        return cls(
            account_id=str(raw.get("account_id") or raw.get("id") or "").strip(),
            auth=MappedAccountAuth.from_dict(raw.get("auth") if isinstance(raw.get("auth"), dict) else raw),
            network=MappedAccountNetwork.from_dict(
                raw.get("network") if isinstance(raw.get("network"), dict) else None
            ),
            storage=MappedAccountStorage.from_dict(
                raw.get("storage") if isinstance(raw.get("storage"), dict) else None
            ),
            browser_type=str(raw.get("browser_type") or "firefox").strip() or "firefox",
            cookie_path=str(raw.get("cookie_path") or "").strip(),
            use_proxy=bool(raw.get("use_proxy", True)),
            status=str(raw.get("status") or "pending").strip() or "pending",
            status_detail=str(raw.get("status_detail") or "").strip(),
            grid_slot_index=int(raw.get("grid_slot_index") or 0),
            soft_login_if_needed=bool(raw.get("soft_login_if_needed", False)),
        )

    def display_uid(self) -> str:
        """UID hiển thị trên GUI."""
        u = self.auth.username or self.account_id
        if u.startswith("UID_"):
            return u[4:]
        return u
