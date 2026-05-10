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

```bash
git pull origin main
```
