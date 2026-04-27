"""
Quản lý đọc/ghi danh sách tài khoản từ file JSON cấu hình.

Tuân thủ quy tắc dự án: không hard-code dữ liệu nhạy cảm trong code,
chỉ thao tác với đường dẫn file được truyền vào hoặc mặc định từ thư mục dự án.
"""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Optional, TypedDict

from loguru import logger

from src.utils.paths import project_root


# Dùng dạng factory vì khóa JSON "pass" trùng từ khóa Python.
ProxyConfig = TypedDict(
    "ProxyConfig",
    {"host": str, "port": int, "user": str, "pass": str},
    total=False,
)


class AccountRecord(TypedDict, total=False):
    """Một bản ghi tài khoản trong accounts.json."""

    id: str
    name: str
    browser_type: str
    portable_path: str
    proxy: ProxyConfig
    cookie_path: str
    schedule_time: str
    status: str
    topic: str
    content_style: str
    post_image_path: str
    last_post_at: str
    login_status: str
    use_proxy: bool
    profile_path: str
    browser_exe_path: str
    import_type: str
    notes: str


def _default_accounts_path() -> Path:
    """
    Trả về đường dẫn mặc định tới config/accounts.json (thư mục gốc dự án).

    Returns:
        Path tới file accounts.json.
    """
    return project_root() / "config" / "accounts.json"


class AccountsDatabaseManager:
    """
    Lớp OOP quản lý đọc/ghi file JSON danh sách tài khoản.

    Attributes:
        file_path: Đường dẫn tuyệt đối tới file JSON.
    """

    REQUIRED_TOP_LEVEL_KEYS: tuple[str, ...] = (
        "id",
        "name",
        "browser_type",
        "portable_path",
        "proxy",
        "cookie_path",
    )
    REQUIRED_PROXY_KEYS: tuple[str, ...] = ("host", "port", "user", "pass")

    def __init__(self, json_path: Optional[Path | str] = None) -> None:
        """
        Khởi tạo bộ quản lý với đường dẫn file JSON.

        Args:
            json_path: Đường dẫn tới accounts.json. Nếu None, dùng config/accounts.json
                tính từ thư mục gốc dự án.
        """
        if json_path is None:
            self.file_path = _default_accounts_path()
        else:
            self.file_path = Path(json_path).resolve()
        logger.debug("AccountsDatabaseManager khởi tạo với file: {}", self.file_path)
        self._rows_cache: Optional[list[AccountRecord]] = None
        self._rows_mtime: Optional[float] = None

    @staticmethod
    def _coerce_use_proxy_flag(record: dict[str, Any]) -> bool:
        v = record.get("use_proxy", True)
        if isinstance(v, bool):
            return v
        if isinstance(v, (int, float)):
            return bool(v)
        s = str(v).strip().lower()
        if s in ("0", "false", "no", "off"):
            return False
        return True

    def _normalize_account_dict(self, raw: dict[str, Any]) -> dict[str, Any]:
        """
        Chuẩn hóa trước khi validate/lưu: ``profile_path`` → ``portable_path``, ``chrome`` → ``chromium``,
        proxy ``null`` → object rỗng, ``username``/``password`` → ``user``/``pass``.
        """
        d = dict(raw)
        pp = str(d.get("portable_path", "")).strip()
        prof = str(d.get("profile_path", "")).strip()
        if not pp and prof:
            d["portable_path"] = prof
        bt = str(d.get("browser_type", "firefox")).strip().lower()
        if bt == "chrome":
            d["browser_type"] = "chromium"
        elif not bt:
            d["browser_type"] = "firefox"
        px = d.get("proxy")
        if px is None:
            d["proxy"] = {"host": "", "port": 0, "user": "", "pass": ""}
        elif isinstance(px, dict):
            p2 = dict(px)
            if str(p2.get("user", "")).strip() == "" and str(p2.get("username", "")).strip():
                p2["user"] = str(p2.get("username", "")).strip()
            if str(p2.get("pass", "")).strip() == "" and str(p2.get("password", "")).strip():
                p2["pass"] = str(p2.get("password", "")).strip()
            d["proxy"] = p2
        return d

    def _validate_account_shape(self, record: dict[str, Any]) -> None:
        """
        Kiểm tra một bản ghi có đủ các trường bắt buộc theo schema mẫu.

        Args:
            record: Dictionary một tài khoản.

        Raises:
            ValueError: Thiếu khóa hoặc proxy không hợp lệ.
        """
        missing = [k for k in self.REQUIRED_TOP_LEVEL_KEYS if k not in record]
        if missing:
            raise ValueError(f"Thiếu các trường bắt buộc: {', '.join(missing)}")
        proxy = record.get("proxy")
        if not isinstance(proxy, dict):
            raise ValueError("Trường 'proxy' phải là object JSON.")
        p_missing = [k for k in self.REQUIRED_PROXY_KEYS if k not in proxy]
        if p_missing:
            raise ValueError(f"Proxy thiếu các trường: {', '.join(p_missing)}")
        use_px = self._coerce_use_proxy_flag(record)
        if use_px:
            host = str(proxy.get("host", "")).strip()
            try:
                port = int(proxy.get("port", 0))
            except (TypeError, ValueError):
                port = 0
            if not host or port <= 0:
                raise ValueError("Khi bật use_proxy, cần host không rỗng và port > 0.")

    def validate_account(self, record: dict[str, Any]) -> None:
        """
        Kiểm tra một bản ghi trước khi lưu (API công khai cho GUI / script).

        Args:
            record: Dictionary một tài khoản.

        Raises:
            ValueError: Schema không hợp lệ.
        """
        self._validate_account_shape(self._normalize_account_dict(record))

    def _atomic_write_text(self, text: str) -> None:
        """
        Ghi toàn bộ nội dung file theo cách an toàn (ghi tạm rồi thay thế).

        Args:
            text: Chuỗi UTF-8 sẽ ghi vào file.

        Raises:
            OSError: Lỗi hệ thống khi ghi file.
        """
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(
            prefix="accounts_",
            suffix=".tmp.json",
            dir=str(self.file_path.parent),
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as tmp:
                tmp.write(text)
                tmp.flush()
                os.fsync(tmp.fileno())
            os.replace(tmp_name, self.file_path)
        except Exception:
            try:
                os.unlink(tmp_name)
            except OSError:
                pass
            raise

    def _invalidate_rows_cache(self) -> None:
        """
        Xóa cache trong bộ nhớ khi không còn đảm bảo khớp với file trên đĩa.
        """
        self._rows_cache = None
        self._rows_mtime = None

    def reload_from_disk(self) -> list[AccountRecord]:
        """
        Bỏ cache và đọc lại ``accounts.json`` từ đĩa (dùng sau khi sửa file thủ công hoặc làm mới GUI).

        Returns:
            Danh sách tài khoản mới nhất.
        """
        self._invalidate_rows_cache()
        return self.load_all()

    def load_all(self) -> list[AccountRecord]:
        """
        Đọc toàn bộ danh sách tài khoản từ file JSON.

        Dùng cache theo ``mtime`` để tránh đọc đĩa lặp khi scheduler / ``get_by_id`` gọi nhiều lần.

        Returns:
            Danh sách các bản ghi tài khoản (list of dict).

        Raises:
            FileNotFoundError: File không tồn tại.
            json.JSONDecodeError: Nội dung không phải JSON hợp lệ.
            ValueError: Cấu trúc không phải mảng hoặc phần tử không phải object.
        """
        if not self.file_path.is_file():
            self._invalidate_rows_cache()
            logger.error("Không tìm thấy file accounts: {}", self.file_path)
            raise FileNotFoundError(str(self.file_path))
        mtime = self.file_path.stat().st_mtime
        if self._rows_cache is not None and self._rows_mtime == mtime:
            logger.debug("load_all: dùng cache ({} tài khoản)", len(self._rows_cache))
            return list(self._rows_cache)

        raw = self.file_path.read_text(encoding="utf-8")
        data = json.loads(raw)
        if not isinstance(data, list):
            raise ValueError("File accounts.json phải là một mảng JSON ở gốc.")
        out: list[AccountRecord] = []
        for idx, item in enumerate(data):
            if not isinstance(item, dict):
                raise ValueError(f"Phần tử index {idx} không phải object JSON.")
            self._validate_account_shape(item)
            out.append(item)  # type: ignore[arg-type]
        self._rows_cache = out
        self._rows_mtime = mtime
        logger.info("Đã đọc {} tài khoản từ {}", len(out), self.file_path)
        return list(out)

    def save_all(self, accounts: Iterable[AccountRecord]) -> None:
        """
        Ghi đè toàn bộ danh sách tài khoản xuống file JSON.

        Args:
            accounts: Iterable các bản ghi đã kiểm tra schema.

        Raises:
            ValueError: Một bản ghi không hợp lệ.
        """
        rows: list[AccountRecord] = []
        for idx, acc in enumerate(accounts):
            if not isinstance(acc, dict):
                raise ValueError(f"Bản ghi index {idx} không phải dict.")
            self._validate_account_shape(acc)
            rows.append(acc)
        text = json.dumps(rows, ensure_ascii=False, indent=2) + "\n"
        self._atomic_write_text(text)
        self._rows_cache = rows
        self._rows_mtime = self.file_path.stat().st_mtime
        logger.info("Đã ghi {} tài khoản xuống {}", len(rows), self.file_path)

    def get_by_id(self, account_id: str) -> Optional[AccountRecord]:
        """
        Lấy một tài khoản theo id.

        Args:
            account_id: Giá trị trường id.

        Returns:
            Bản ghi nếu tìm thấy, ngược lại None.
        """
        for acc in self.load_all():
            if acc.get("id") == account_id:
                logger.debug("Tìm thấy tài khoản id={}", account_id)
                return acc
        logger.warning("Không tìm thấy tài khoản id={}", account_id)
        return None

    def upsert(self, account: AccountRecord) -> None:
        """
        Thêm mới hoặc cập nhật một tài khoản theo id rồi lưu file.

        Args:
            account: Bản ghi đầy đủ trường theo schema.

        Raises:
            ValueError: Thiếu id hoặc schema không hợp lệ.
        """
        normalized = self._normalize_account_dict(dict(account))
        self._validate_account_shape(normalized)
        acc_id = normalized.get("id")
        if not acc_id:
            raise ValueError("Bản ghi phải có trường 'id' không rỗng.")
        current = self.load_all()
        replaced = False
        new_list: list[AccountRecord] = []
        for row in current:
            if row.get("id") == acc_id:
                new_list.append(normalized)  # type: ignore[arg-type]
                replaced = True
            else:
                new_list.append(row)
        if not replaced:
            new_list.append(normalized)  # type: ignore[arg-type]
            logger.info("Thêm tài khoản mới id={}", acc_id)
        else:
            logger.info("Cập nhật tài khoản id={}", acc_id)
        self.save_all(new_list)

    def update_account_fields(self, account_id: str, updates: dict[str, Any]) -> None:
        """
        Gộp các trường ``updates`` vào bản ghi theo ``account_id`` rồi lưu lại file JSON.

        Args:
            account_id: id tài khoản.
            updates: Dict các trường cần cập nhật (ví dụ ``status``, ``last_post_at``).

        Raises:
            ValueError: Không tìm thấy id hoặc sau gộp không còn hợp lệ schema.
        """
        rows = self.load_all()
        current = next((r for r in rows if r.get("id") == account_id), None)
        if current is None:
            raise ValueError(f"Không tìm thấy tài khoản: {account_id}")
        merged: dict[str, Any] = self._normalize_account_dict({**dict(current), **updates})
        self._validate_account_shape(merged)
        new_list: list[AccountRecord] = []
        for row in rows:
            if row.get("id") == account_id:
                new_list.append(merged)  # type: ignore[arg-type]
            else:
                new_list.append(row)
        self.save_all(new_list)
        logger.info("Đã cập nhật trường {} cho id={}", list(updates.keys()), account_id)

    def record_post_outcome(self, account_id: str, *, success: bool) -> None:
        """
        Cập nhật ``status`` và (khi thành công) ``last_post_at`` sau một phiên đăng bài.

        Thành công: ``status=success``, ``last_post_at`` theo giờ máy (YYYY-MM-DD HH:MM),
        ``login_status=active``. Thất bại: chỉ ``status=failed`` (giữ nguyên ``last_post_at`` nếu có).
        """
        if success:
            stamp = datetime.now().strftime("%Y-%m-%d %H:%M")
            self.update_account_fields(
                account_id,
                {
                    "status": "success",
                    "last_post_at": stamp,
                    "login_status": "active",
                },
            )
        else:
            self.update_account_fields(account_id, {"status": "failed"})

    def delete_by_id(self, account_id: str) -> bool:
        """
        Xóa tài khoản theo id nếu tồn tại.

        Args:
            account_id: id cần xóa.

        Returns:
            True nếu đã xóa ít nhất một bản ghi, False nếu không có id đó.
        """
        current = self.load_all()
        new_list = [a for a in current if a.get("id") != account_id]
        if len(new_list) == len(current):
            logger.warning("Không có tài khoản để xóa với id={}", account_id)
            return False
        self.save_all(new_list)
        logger.info("Đã xóa tài khoản id={}", account_id)
        return True


def update_last_post_time(account_id: str, manager: Optional[AccountsDatabaseManager] = None) -> None:
    """
    Ghi nhận đăng bài thành công: cập nhật ``last_post_at`` và ``status=success``.

    Args:
        account_id: ``id`` trong ``accounts.json``.
        manager: Bộ quản lý JSON; mặc định dùng ``AccountsDatabaseManager()`` với đường dẫn chuẩn.
    """
    db = manager if manager is not None else AccountsDatabaseManager()
    db.record_post_outcome(account_id, success=True)
