# ToolFB

Công cụ hỗ trợ Facebook + lên lịch bài + AI (Gemini / Veo) + tải video (yt-dlp), v.v.

## Cài đặt nhanh (máy mới)

1. **Clone / cập nhật**
   ```bash
   git pull origin main
   ```
2. **Python** (3.10+ khuyên dùng) — cài dependency nếu project có `requirements.txt` / dùng môi trường ảo.
3. **Chạy**
   ```bash
   python main.py --gui
   ```

## Veo3 / Veo3Studio (cài local — không có trong repo)

Thư mục **`tools/Veo3Studio/`** bị `.gitignore` để repo nhẹ và tránh binary lớn trên GitHub.

**Máy mới sau khi clone:** cài bản Veo3Studio Lite (hoặc copy từ máy khác) vào `ToolFB/tools/Veo3Studio/`, đảm bảo có `Veo3Studio.exe` (hoặc chỉnh đường dẫn trong tab **AI Video** → `tool_exe`).

## Cấu hình riêng từng máy (không commit)

Các file như `config/app_secrets.json`, tài khoản, lịch đăng, v.v. được `.gitignore` — mỗi máy tự cấu hình (có thể dùng `config/app_secrets.example.json` làm mẫu).

## Cập nhật từ GitHub

**Mã nguồn (git):**

```bash
git pull origin main
```

**Trong GUI — «Kiểm tra cập nhật» / «Cập nhật ngay»**

- Repo đã kèm `config/update_channel.json` trỏ tới `…/releases/latest/download/latest.json` (GitHub Release **mới nhất** phải đính kèm file `latest.json` và gói zip trong manifest).
- Bấm **Kiểm tra cập nhật**: nếu có bản mới, đóng hộp thoại rồi bấm **Cập nhật ngay** để tải gói và áp dụng (bản `.exe` Windows có thể cần **mở lại chương trình** để script nền thay `ToolFB_GUI.exe` / `_internal`).
- Máy không có `.git` hoặc đổi kênh: **Cấu hình kênh cập nhật** (hoặc biến môi trường `TOOLFB_UPDATE_MANIFEST_URL`). Mẫu: `config/update_channel.example.json`.

**Người phát hành:** khi đẩy bản mới, cập nhật `version.json` trong repo, tạo/đổi GitHub Release, đính kèm `ToolFB_release_bundle.zip` và `latest.json` (đúng `version`, `download_url`, `sha256` của zip) để các máy khách nhận bản mới khi kiểm tra cập nhật.
