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

## Veo3 / Veo3Studio (máy khác — bắt buộc tự thêm)

Thư mục `tools/Veo3Studio/` **không nằm trong Git** (dung lượng lớn). Trên mỗi máy cần:

- Copy toàn bộ bản build **Veo3Studio** (có `Veo3Studio.exe`) vào:
  - `ToolFB/tools/Veo3Studio/`
- Hoặc đặt `Veo3Studio.exe` ở đâu cũng được, rồi trong app mở **AI Video** → chỉnh đường dẫn **tool / Veo3Studio.exe** (biến nội bộ `tool_exe`).

Sau khi file `.exe` tồn tại, luồng bridge / mở Veo3 từ app sẽ chạy được.

## Cấu hình riêng từng máy (không commit)

Các file như `config/app_secrets.json`, tài khoản, lịch đăng, v.v. được `.gitignore` — mỗi máy tự cấu hình (có thể dùng `config/app_secrets.example.json` làm mẫu).

## Cập nhật từ GitHub

`git pull` trên `main` để lấy code mới; Veo3Studio vẫn cần bản local như trên (hoặc gói phát hành riêng nếu bạn dùng `dist`/release).
