"""
Validation và log bước cho pipeline đăng bài Facebook (job queue).

Tuân thủ tiến trình chuẩn: kiểm tra account/page/job trước khi mở browser; log tiếng Việt theo bước.
"""

from __future__ import annotations

import json
import re
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from loguru import logger

from src.automation.browser_factory import (
    _project_root,
    account_use_proxy_enabled,
    proxy_host_port_configured,
)

# Các bước runtime (tham chiếu spec) — chỉ dùng để log thống nhất.
STEP_INIT = "INIT"
STEP_LOAD_JOB = "LOAD_JOB"
STEP_LOAD_ACCOUNT = "LOAD_ACCOUNT"
STEP_LOAD_PAGE = "LOAD_PAGE"
STEP_VALIDATE_ACCOUNT = "VALIDATE_ACCOUNT"
STEP_VALIDATE_PAGE = "VALIDATE_PAGE"
STEP_VALIDATE_JOB = "VALIDATE_JOB"
STEP_OPEN_BROWSER = "OPEN_BROWSER"
STEP_SESSION_ENSURE = "SESSION_ENSURE"
STEP_NAV_TARGET = "NAV_TARGET"
STEP_COMPOSER = "COMPOSER"
STEP_FILL_CONTENT = "FILL_CONTENT"
STEP_MEDIA = "MEDIA"
STEP_SUBMIT = "SUBMIT"
STEP_VERIFY_RESULT = "VERIFY_RESULT"

_JOB_RUN_MONITOR_LOCK = threading.Lock()


def log_job_step(step: str, message: str, **ctx: Any) -> None:
    """Ghi log một bước job (tiếng Việt), kèm context tùy chọn."""
    if ctx:
        logger.info("[Job:{}] {} | {}", step, message, ctx)
    else:
        logger.info("[Job:{}] {}", step, message)


def job_run_monitor_path(*, project_root: Path | None = None) -> Path:
    root = project_root or _project_root()
    d = root / "data" / "runtime"
    d.mkdir(parents=True, exist_ok=True)
    return d / "job_run_monitor.json"


class JobRunTracker:
    """Ghi bước FSM runtime ra ``data/runtime/job_run_monitor.json`` (UI monitor đọc file này)."""

    def __init__(self, job_id: str | None, *, project_root: Path | None = None) -> None:
        self._root = project_root or _project_root()
        self.job_id = str(job_id or "").strip() or "—"

    def set_step(self, step: str, message: str = "", **extra: Any) -> None:
        log_job_step(step, message, job_id=self.job_id, **extra)
        payload: dict[str, Any] = {
            "job_id": self.job_id,
            "step": step,
            "message": message,
            "updated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        }
        for k, v in extra.items():
            if v is not None:
                try:
                    json.dumps(v)
                    payload[k] = v
                except (TypeError, ValueError):
                    payload[k] = str(v)
        p = job_run_monitor_path(project_root=self._root)
        with _JOB_RUN_MONITOR_LOCK:
            p.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _acc_status_active(acc: dict[str, Any]) -> bool:
    st = str(acc.get("status", "active")).strip().lower()
    return st in ("", "active", "ok", "enabled")


def validate_account_for_post_job(acc: dict[str, Any], *, project_root: Path | None = None) -> None:
    """
    Kiểm tra account trước khi mở browser.

    Raises:
        ValueError: Thiếu cấu hình hoặc đường dẫn không hợp lệ (thông báo tiếng Việt).
    """
    root = project_root or _project_root()
    if not _acc_status_active(acc):
        raise ValueError(
            f"Tài khoản {acc.get('id')} không ở trạng thái active (status={acc.get('status')!r})."
        )
    exe = str(acc.get("browser_exe_path", "")).strip()
    if exe:
        pexe = Path(exe)
        if not pexe.is_file():
            raise ValueError(f"browser_exe_path không tồn tại: {exe}")
    raw_profile = str(acc.get("portable_path", "") or acc.get("profile_path", "")).strip()
    if not raw_profile:
        raise ValueError("Thiếu portable_path / profile_path cho tài khoản.")
    prof = Path(raw_profile)
    if not prof.is_absolute():
        prof = (root / prof).resolve()
    if not prof.is_dir():
        raise ValueError(
            f"Thư mục profile không tồn tại: {prof}. Không tự tạo profile mới trong bước đăng bài."
        )
    use_px = account_use_proxy_enabled(acc)
    px = acc.get("proxy")
    if use_px:
        if not isinstance(px, dict):
            raise ValueError("Cấu hình proxy không hợp lệ (phải là object).")
        if not proxy_host_port_configured(px):
            raise ValueError("use_proxy=true nhưng proxy thiếu host/port hợp lệ.")
    ck = str(acc.get("cookie_path", "")).strip()
    if ck:
        cp = Path(ck)
        if not cp.is_absolute():
            cp = (root / cp).resolve()
        if not cp.is_file():
            raise ValueError(f"cookie_path không trỏ tới file hợp lệ: {cp}")


def validate_page_for_post_job(page_row: dict[str, Any], account_id: str) -> None:
    """Kiểm tra page thuộc account và đủ dữ liệu điều hướng."""
    if str(page_row.get("account_id", "")).strip() != str(account_id).strip():
        raise ValueError(
            f"Page {page_row.get('id')} không thuộc account_id={account_id}."
        )
    url = str(page_row.get("page_url", "")).strip()
    name = str(page_row.get("page_name", "")).strip()
    if not url and not name:
        raise ValueError(f"Page {page_row.get('id')} thiếu page_url và page_name.")


_IMG_EXT = re.compile(r"\.(jpe?g|png|webp)$", re.I)
_VID_EXT = re.compile(r"\.(mp4|mov|avi|mkv)$", re.I)


def validate_queue_job_payload(
    job: dict[str, Any],
    *,
    project_root: Path | None = None,
) -> None:
    """
    Kiểm tra job queue: post_type, file media tồn tại.

    Raises:
        ValueError: Dữ liệu job không hợp lệ.
    """
    root = project_root or _project_root()
    pt = str(job.get("post_type", "text")).strip().lower()
    media = job.get("media_files") or []
    paths: list[str] = [str(p).strip() for p in media if str(p).strip()] if isinstance(media, list) else []

    if pt in ("image", "text_image"):
        if not paths:
            raise ValueError("Job ảnh cần media_files không rỗng.")
        for rel in paths:
            p = Path(rel)
            if not p.is_absolute():
                p = (root / p).resolve()
            if not p.is_file():
                raise ValueError(f"File ảnh không tồn tại: {p}")
            if not _IMG_EXT.search(p.name):
                raise ValueError(f"Định dạng ảnh không hỗ trợ: {p.name}")
    elif pt in ("video", "text_video"):
        if not paths:
            raise ValueError("Job video cần media_files không rỗng.")
        if len(paths) > 1:
            logger.warning(
                "[Job:VALIDATE_JOB] Nhiều video trong một job — chuẩn an toàn là 1 video/job; sẽ chỉ xử lý file đầu."
            )
        for rel in paths[:1]:
            p = Path(rel)
            if not p.is_absolute():
                p = (root / p).resolve()
            if not p.is_file():
                raise ValueError(f"File video không tồn tại: {p}")
            if not _VID_EXT.search(p.name):
                raise ValueError(f"Định dạng video không hỗ trợ: {p.name}")
