"""Hợp đồng tạo Page — không tự vượt checkpoint / CAPTCHA."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Protocol


@dataclass
class PageCreationOutcome:
    """Kết quả một lần tạo hoặc xác minh Page."""

    ok: bool = False
    page_id: str = ""
    page_url: str = ""
    error_code: str = ""
    error_message: str = ""
    page_name: str = ""

    @property
    def requires_user_action(self) -> bool:
        return self.error_code in {
            "SESSION_EXPIRED",
            "CAPTCHA_DETECTED",
            "CHECKPOINT",
            "RATE_LIMITED",
            "SMS_VERIFICATION_REQUIRED",
            "SECURITY_VERIFICATION_REQUIRED",
        }


@dataclass
class PageCreationRequest:
    """Dữ liệu một job đưa cho provider."""

    job_id: str
    batch_id: str
    account_id: str
    business_id: str
    page_name: str
    avatar_path: str
    create_mode: str = "bm"
    admin_targets: list[str] = field(default_factory=list)
    category: str = ""
    bio: str = ""
    phone: str = ""
    email: str = ""
    website: str = ""
    address: str = ""
    should_stop: Callable[[], bool] = field(default=lambda: False)
    on_state: Callable[[str], None] = field(default=lambda _s: None)
    on_progress: Callable[[str], None] = field(default=lambda _m: None)


class PageCreationProvider(Protocol):
    """Tạo và xác minh Page. CAPTCHA/checkpoint phải trả mã dừng, không giải tự động."""

    def check_session(self, account_id: str) -> PageCreationOutcome:
        """Phiên còn dùng được hay cần người dùng đăng nhập lại."""

    def verify_existing(self, request: PageCreationRequest) -> PageCreationOutcome:
        """Tìm Page đã tạo (cùng tên + BM) để tránh tạo trùng sau timeout."""

    def create_and_verify(self, request: PageCreationRequest) -> PageCreationOutcome:
        """Tạo Page, tải avatar, xác minh có page_id/url. Không đánh dấu thành công chỉ vì đã bấm Create."""
