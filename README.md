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

## Veo3 / Veo3Studio (trong repo — Git LFS)

`tools/Veo3Studio/` được đưa lên GitHub; file **`Veo3Studio.exe`** (~180MB) dùng **Git LFS** (GitHub giới hạn 100MB/file với git thường).

**Máy mới sau khi clone:**

1. Cài [Git LFS](https://git-lfs.com/) (một lần): `git lfs install`
2. Clone như bình thường — nếu exe chỉ vài trăm byte (pointer), chạy tại thư mục repo: `git lfs pull`
3. Đường dẫn chuẩn: `ToolFB/tools/Veo3Studio/Veo3Studio.exe`

Thư mục `data/` bên trong Veo3Studio không nằm trong Git (dữ liệu chạy từng máy). Nếu cần chỉnh tay: tab **AI Video** → đường dẫn `tool_exe`.

## Cấu hình riêng từng máy (không commit)

Các file như `config/app_secrets.json`, tài khoản, lịch đăng, v.v. được `.gitignore` — mỗi máy tự cấu hình (có thể dùng `config/app_secrets.example.json` làm mẫu).

## Cập nhật từ GitHub

```bash
git pull origin main
git lfs pull
```

Nếu `Veo3Studio.exe` không chạy được sau pull, kiểm tra đã cài Git LFS và chạy `git lfs pull`.
