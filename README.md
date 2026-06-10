# ToolFB

Công cụ tự động Facebook: lịch đăng bài, quản lý Page/Group, AI (Gemini), tải video (yt-dlp), chỉnh video, TikTok — giao diện Windows (Tkinter).

## Yêu cầu

- **Windows 10/11** (khuyên dùng; dev có thể chạy Python trực tiếp)
- **Python 3.10+** (3.11–3.12 ổn định)
- **RAM** 8GB+ (16GB nếu vừa tải video + chỉnh video + đăng bài song song)
- **FFmpeg** — tùy chọn; app có thể dùng `tools/ffmpeg/bin` hoặc FFmpeg trên PATH

## Cài đặt từ GitHub (máy mới)

```bat
git clone https://github.com/<ORG>/ToolFB.git
cd ToolFB
scripts\setup_windows.bat
```

Hoặc thủ công:

```bat
python -m venv .venv
.venv\Scripts\pip install -r requirements.txt
.venv\Scripts\python -m playwright install firefox
.venv\Scripts\python main.py --gui
```

Lần đầu mở app sẽ **tự tạo** `config/accounts.json`, `pages.json`, … từ file `*.example.json` (không ghi đè nếu đã có).

## Chạy chương trình

| Cách | Lệnh |
|------|------|
| GUI (khuyên dùng) | `Start_ToolFB_GUI.bat` hoặc `python main.py --gui` |
| Scheduler terminal | `python main.py` (dev) / `ToolFB_GUI.exe --cli` (bản exe) |
| Nhiều cửa sổ | `python main.py --gui --multi-instance --data-dir D:\ToolFB_2` |

## Thiết lập lần đầu (trong GUI)

1. Tab **Tài khoản** → **Thêm** — profile Firefox, cookie_path, portable_path  
2. Tab **Page / Group** — gắn Page với `account_id`  
3. Tab **Cài đặt AI** — dán Gemini API key (hoặc sửa `config/app_secrets.json` từ mẫu)  
4. Tab **Job lịch** — tạo job; **Bắt đầu lịch** khi sẵn sàng  
5. (Tùy chọn) **Facebook đăng nhập / TOTP** trong form sửa tài khoản — vault `config/account_credentials.json`

Nút **Hướng dẫn** trên thanh công cụ mở checklist nhanh.

## Đa tác vụ (download + Video Editor + đăng bài)

App **tự giảm** tải browser/FFmpeg khi nhiều chức năng chạy cùng lúc (`TOOLFB_AUTO_MULTITASK=1`, mặc định bật).

| Biến môi trường | Ý nghĩa |
|-----------------|--------|
| `BROWSER_CONCURRENCY` | Số browser đăng song song (mặc định ~ CPU/2) |
| `TOOLFB_FFMPEG_CONCURRENCY` | Số FFmpeg song song (1–2) |
| `TOOLFB_MANUAL_CONCURRENCY` | `1` = tắt tự điều chỉnh |
| `TOOLFB_DATA_DIR` | Thư mục dữ liệu riêng khi mở nhiều instance |

## File không đưa lên Git (mỗi máy tự có)

Đã khai báo trong `.gitignore`:

- `config/accounts.json`, `pages.json`, `schedule_posts.json`, `app_secrets.json`, …
- `config/account_credentials.json` (mật khẩu / TOTP)
- `data/` (profile, cookie, tải video)
- `logs/`
- `tools/Veo3Studio/`, `tools/ffmpeg/` (binary lớn)

Dùng file **`config/*.example.json`** làm mẫu.

## Veo3 / Veo3Studio

Thư mục `tools/Veo3Studio/` **không** có trong repo. Cài tay hoặc copy từ máy khác; chỉnh `tool_exe` trong tab **AI Video**.

## Cập nhật

**Máy đã clone repo:** Mặc định app **tự `git pull`** khi mở (bản clone, working tree sạch). Tắt: `set TOOLFB_AUTO_GIT_PULL=0` hoặc `"git_pull_on_startup": false` trong `config/auto_update.json` (mẫu: `config/auto_update.example.json`). Sau pull nên **khởi động lại app**.

**Task Scheduler (không mở app):** chạy `tools\auto_git_pull.ps1` hoặc `.venv\Scripts\python.exe tools\sync_from_github.py --force`.

**Git thủ công:** `git pull origin main`

**Trong app:** *Kiểm tra cập nhật* / *Cập nhật* — bản zip cần release GitHub kèm `latest.json` (xem `config/update_channel.example.json`).

**Phát hành:** workflow `.github/workflows/release.yml` hoặc `Publish_Update_GitHub.bat`.

## Kiểm thử

```bat
.venv\Scripts\python.exe -m pytest tests/ -q
```

## Cấu trúc chính

```
main.py              # Entry GUI / CLI
src/gui/             # Giao diện
src/scheduler.py     # Lịch đăng + browser pool
src/automation/      # Playwright Facebook
src/services/        # Post, download, video editor
config/              # Cấu hình (*.example.json trong repo)
data/                # Runtime (gitignore)
```

## Giấy phép / hỗ trợ

Dùng nội bộ / theo quy định repo. Báo lỗi qua Issues trên GitHub kèm log `logs/` và phiên bản (hiển thị góc GUI).
