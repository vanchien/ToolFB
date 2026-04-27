"""
Điểm vào chương trình: điều phối lịch đăng + log trạng thái trên terminal.

Chạy: ``python main.py`` (chỉ terminal), ``python main.py --gui`` (Tkinter), ``ToolFB_GUI.exe`` (bản đóng gói: mặc định GUI; ``ToolFB_GUI.exe --cli`` = scheduler terminal), hoặc ``streamlit run dashboard.py`` (dashboard web).

Biến môi trường quan trọng:
- ``GEMINI_API_KEY``: bắt buộc để sinh nội dung.
- ``HEADLESS``: mặc định ``1`` (headless) phù hợp chạy 24/7.
- ``BROWSER_CONCURRENCY``: số trình duyệt tối đa đồng thời (mặc định 3).
- ``GEMINI_MODEL``: tên model (mặc định ``gemini-2.5-flash``; 2.0-flash không còn cho user mới).
- ``GEMINI_IMAGE_MODEL``: model Imagen sinh ảnh (mặc định thử ``imagen-3.0-generate-002``).
- ``NANOBANANA_API_KEY``: nếu có sẽ ưu tiên dùng NanoBanana để sinh ảnh.
- ``NANOBANANA_API_KEYS``: nhiều key NanoBanana (phân tách dấu phẩy) để phân tải gen ảnh nhanh hơn.
- ``VEO3_API_KEY`` / ``VEO3_API_KEYS``: alias tương thích, dùng như pool key NanoBanana.
- ``NANOBANANA_API_URL``: endpoint generate (mặc định ``/api/v1/nanobanana/generate``).
- ``NANOBANANA_RECORD_INFO_URL``: endpoint poll task (mặc định ``/api/v1/nanobanana/record-info?taskId=...``).
- ``NANOBANANA_USE_BROWSER``: ``1`` (mặc định) = tạo ảnh bằng browser automation profile riêng.
- ``NANOBANANA_WEB_URL``: URL trang web NanoBanana để automation.
- ``NANOBANANA_BROWSER_PROFILE_DIR``: thư mục profile trình duyệt NanoBanana.
- Nếu NanoBanana/Imagen lỗi, hệ thống fallback Pollinations để vẫn tự sinh ảnh.
- ``SCHEDULER_TZ``: IANA timezone cho giờ đăng (mặc định ``Asia/Ho_Chi_Minh``).
- ``SCHEDULER_POOL_THREADS``: số worker thread cho job (mặc định ``16``).
- ``STATUS_LOG_INTERVAL_SEC``: chu kỳ log tổng quan tài khoản (tối thiểu ``10``).
- ``DISABLE_PROFILE_CLEANUP``: đặt ``1`` để **không** dọn profile mồ côi khi khởi động (mặc định: xóa thư mục ``data/profiles/...`` không còn trong accounts **và** file ``data/cookies/<tên_profile>.json`` đi kèm nếu không còn tài khoản nào trỏ tới).
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


def _configure_frozen_runtime() -> None:
    """
    PyInstaller: trình duyệt Playwright nằm trong ``_internal/ms-playwright``;
    Chromium dùng bản bundle (không bắt buộc cài Chrome trên máy lạ).
    """
    if not getattr(sys, "frozen", False):
        return
    exe_dir = Path(sys.executable).resolve().parent
    for rel in ("_internal/ms-playwright", "ms-playwright"):
        p = exe_dir / rel
        if p.is_dir() and any(p.iterdir()):
            os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", str(p.resolve()))
            break
    os.environ.setdefault("FB_PLAYWRIGHT_CHROMIUM_CHANNEL", "bundled")


_configure_frozen_runtime()

from loguru import logger

from src.utils.app_secrets import (
    apply_saved_gemini_key_to_environ,
    apply_saved_openai_key_to_environ,
    apply_saved_nanobanana_config_to_environ,
    apply_saved_nanobanana_key_to_environ,
)
from src.scheduler import run_forever
from src.utils.db_manager import AccountsDatabaseManager, _default_accounts_path
from src.utils.profile_cleanup import cleanup_orphan_profile_directories
from src.utils.runtime_cleanup import cleanup_runtime_junk


def _ensure_minimal_config_for_first_run() -> None:
    """
    Sau khi clone git, thường thiếu ``config/accounts.json`` → ``main.py`` thoát ngay, GUI không lên.
    Tạo ``config/`` và file accounts rỗng ``[]`` nếu chưa có (không ghi đè file đã tồn tại).
    """
    from src.utils.paths import project_root

    root = project_root()
    cfg = root / "config"
    cfg.mkdir(parents=True, exist_ok=True)
    path = _default_accounts_path()
    if not path.is_file():
        path.write_text("[]\n", encoding="utf-8")
        logger.info(
            "Đã tạo {} rỗng (máy mới / clone repo). Mở tab Tài khoản để thêm profile.",
            path,
        )


def _preflight_or_exit() -> AccountsDatabaseManager:
    """
    Kiểm tra nhanh trước khi chạy 24/7: file cấu hình tồn tại, cảnh báo thiếu Gemini API key.

    Returns:
        ``AccountsDatabaseManager`` đã warm cache (dùng lại cho scheduler, tránh đọc JSON lặp).
    """
    path = _default_accounts_path()
    if not path.is_file():
        logger.error("Thiếu file cấu hình: {} — hãy tạo trước khi chạy (hoặc chạy lại bản app đã có bước bootstrap).", path)
        raise SystemExit(1)
    mgr = AccountsDatabaseManager()
    rows = mgr.load_all()
    try:
        cleanup_orphan_profile_directories(rows)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Dọn profile mồ côi bị lỗi (bỏ qua): {}", exc)
    if not os.environ.get("GEMINI_API_KEY", "").strip():
        logger.warning(
            "Chưa đặt GEMINI_API_KEY — tới giờ lịch, bước AI sẽ lỗi và ghi vào logs/failed_accounts.log."
        )
    return mgr


def _configure_stdio_utf8() -> None:
    """
    Tránh ``UnicodeEncodeError`` khi in argparse / log trên Windows (mặc định cp1252).
    """
    for stream in (sys.stdout, sys.stderr):
        if stream is None:
            continue
        reconf = getattr(stream, "reconfigure", None)
        if callable(reconf):
            try:
                reconf(encoding="utf-8", errors="replace")
            except (OSError, ValueError, AttributeError):
                pass


def _configure_logging() -> None:
    """
    Cấu hình Loguru ra stderr với định dạng dễ đọc trên terminal (UI log trạng thái).

    PyInstaller ``--windowed``: ``sys.stderr`` thường là ``None`` — ghi file fallback
    (``logs/toolfb_loguru_fallback.log`` cạnh ``.exe``) để không crash khi mở trên máy khác.
    """
    from src.utils.paths import project_root

    logger.remove()
    sink: object = sys.stderr
    if sink is None:
        sink = getattr(sys, "__stderr__", None)
    colorize = sink is not None
    if sink is None:
        log_path = project_root() / "logs" / "toolfb_loguru_fallback.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        sink = str(log_path)
    logger.add(
        sink,
        colorize=colorize,
        level="INFO",
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level:<8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>",
    )


def _cleanup_previous_background_instances() -> None:
    """
    Windows EXE mode: tự dọn các tiến trình ToolFB_GUI.exe cũ trước khi chạy.

    Mục tiêu: tránh treo/đụng tài nguyên khi người dùng bấm mở app nhiều lần
    hoặc còn tiến trình nền từ phiên trước.
    """
    if os.name != "nt":
        return
    if not getattr(sys, "frozen", False):
        return
    if os.environ.get("TOOLFB_DISABLE_STARTUP_CLEANUP", "").strip() in {"1", "true", "yes", "on"}:
        logger.info("Bỏ qua dọn tiến trình nền do TOOLFB_DISABLE_STARTUP_CLEANUP=1")
        return
    current_pid = os.getpid()
    exe_name = Path(sys.executable).name or "ToolFB_GUI.exe"
    cmd = [
        "taskkill",
        "/F",
        "/T",
        "/FI",
        f"IMAGENAME eq {exe_name}",
        "/FI",
        f"PID ne {current_pid}",
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if proc.returncode == 0:
            logger.info("Đã dọn tiến trình {} cũ trước khi khởi động.", exe_name)
            return
        merged = " ".join(
            s.strip()
            for s in ((proc.stdout or ""), (proc.stderr or ""))
            if s and s.strip()
        ).lower()
        # Khi không có tiến trình phù hợp, taskkill thường trả mã khác 0.
        if "not found" in merged or "no running instance" in merged or "không tìm thấy" in merged:
            logger.info("Không có tiến trình {} cũ cần dọn.", exe_name)
            return
        logger.warning("Dọn tiến trình {} trả mã {}: {}", exe_name, proc.returncode, merged or "unknown")
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không thể dọn tiến trình nền lúc startup: {}", exc)


def main() -> None:
    """
    Khởi động CLI hoặc GUI theo tham số dòng lệnh.
    """
    _configure_stdio_utf8()
    parser = argparse.ArgumentParser(description="Facebook Automation (AI + lịch + Playwright)")
    parser.add_argument(
        "--gui",
        action="store_true",
        help="Mở bảng điều khiển Tkinter (xem log, bật/tắt lịch, làm mới tài khoản)",
    )
    parser.add_argument(
        "--cli",
        action="store_true",
        help="Bản .exe (PyInstaller): chạy scheduler 24/7 trên terminal, không mở GUI. Bỏ qua tham số này khi dev bằng python.",
    )
    args = parser.parse_args()

    _configure_logging()
    _cleanup_previous_background_instances()
    cleanup_runtime_junk()
    logger.info("Facebook Automation — Giai đoạn 4 (AI + lịch). Đang khởi động...")
    _ensure_minimal_config_for_first_run()
    apply_saved_gemini_key_to_environ()
    apply_saved_openai_key_to_environ()
    apply_saved_nanobanana_key_to_environ()
    apply_saved_nanobanana_config_to_environ()
    accounts = _preflight_or_exit()

    # Bản .exe: double-click / shortcut thường không có argv → phải mặc định GUI.
    # Dev: ``python main.py`` không cờ → scheduler (giữ hành vi cũ).
    is_frozen = getattr(sys, "frozen", False)
    use_gui = (not is_frozen and args.gui) or (is_frozen and not args.cli)
    if is_frozen and not args.gui and not args.cli:
        logger.info("Bản đóng gói: mở GUI mặc định (chạy scheduler terminal: thêm --cli).")

    if use_gui:
        from src.gui.manager_app import run_manager_gui

        run_manager_gui(accounts=accounts)
        return

    run_forever(accounts=accounts)


if __name__ == "__main__":
    main()
