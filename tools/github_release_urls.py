"""URL asset GitHub Release (nhánh ``releases/latest/download``)."""

from __future__ import annotations


def github_latest_asset_url(repo: str, filename: str) -> str:
    """
    URL cố định tới một file đính kèm của release *mới nhất* (non-draft).

    Ví dụ manifest cho updater::
        https://github.com/owner/ToolFB/releases/latest/download/latest.json

    Lưu ý: repo phải public (hoặc máy client có cách tải được URL), vì app chỉ dùng HTTP thường.
    """
    r = (repo or "").strip().strip("/")
    return f"https://github.com/{r}/releases/latest/download/{filename}"
