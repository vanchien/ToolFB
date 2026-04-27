"""Quản lý tài khoản (profile, proxy, cookie) — ủy quyền ``AccountsDatabaseManager``."""

from __future__ import annotations

from typing import Any, Iterable, Optional

from src.models.account import AccountRecord
from src.utils.db_manager import AccountsDatabaseManager


class AccountService:
    """API domain mỏng cho ``config/accounts.json``."""

    def __init__(self, db: Optional[AccountsDatabaseManager] = None) -> None:
        self._db = db or AccountsDatabaseManager()

    def reload_from_disk(self) -> list[AccountRecord]:
        return self._db.reload_from_disk()

    def load_all(self) -> list[AccountRecord]:
        return self._db.load_all()

    def get_by_id(self, account_id: str) -> Optional[AccountRecord]:
        return self._db.get_by_id(account_id)

    def upsert(self, account: AccountRecord) -> None:
        self._db.upsert(account)

    def save_all(self, accounts: Iterable[AccountRecord]) -> None:
        self._db.save_all(accounts)

    def update_account_fields(self, account_id: str, updates: dict[str, Any]) -> None:
        self._db.update_account_fields(account_id, updates)

    def delete_by_id(self, account_id: str) -> bool:
        return self._db.delete_by_id(account_id)
