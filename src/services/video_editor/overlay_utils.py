"""Logo / overlay — kích thước, resolve media, kiểm tra trước export."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any


def normalize_path_for_ffmpeg(path: Path) -> str:
    """Chuẩn hoá đường dẫn cho FFmpeg; Windows: bơm \\\\?\\ khi dài."""
    try:
        resolved = path.expanduser().resolve(strict=False)
    except (OSError, RuntimeError):
        resolved = path.expanduser()
    s = os.path.normpath(str(resolved))
    if os.name != "nt":
        return s
    if str(os.environ.get("TOOLFB_FFMPEG_NO_LONGPATH", "0") or "0").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    ):
        return s
    if s.startswith("\\\\?\\"):
        return s
    if len(s) <= 220:
        return s
    if len(s) >= 2 and s[1] == ":":
        return "\\\\?\\" + s
    if s.startswith("\\\\"):
        return "\\\\?\\UNC\\" + s[2:]
    return s


def resolve_media_file_path(media: dict[str, Any]) -> Path | None:
    """
    Tìm file logo/ảnh trên đĩa — ưu tiên ``local_path`` (thư viện project) rồi ``path`` gốc.
    Thử lại với long-path trên Windows (một số máy không ``is_file()`` được path >260 ký tự).
    """
    lp = str(media.get("local_path") or "").strip()
    op = str(media.get("path") or "").strip()
    for candidate in (lp, op):
        if not candidate:
            continue
        p = Path(candidate).expanduser()
        try:
            if p.is_file():
                return p.resolve()
        except OSError:
            pass
        if os.name == "nt":
            try:
                np = Path(normalize_path_for_ffmpeg(p))
                if np.is_file():
                    return np
            except OSError:
                pass
    return None


def logo_corner_xy_from_label(
    pos_pick: str,
    canvas_w: int,
    canvas_h: int,
    logo_w: int,
    logo_h: int | None = None,
    *,
    margin_x_ratio: float = 0.035,
    margin_y_ratio: float = 0.035,
    min_margin_px: int = 12,
) -> tuple[int, int]:
    """
    Góc logo theo % khung canvas (không cố định 24px) — đổi kích logo vẫn giữ vị trí tương đối.
    """
    pw = max(1, int(canvas_w))
    ph = max(1, int(canvas_h))
    lw = max(1, int(logo_w))
    lh = max(1, int(logo_h if logo_h is not None else logo_w))
    mx = max(min_margin_px, int(round(pw * max(0.01, min(0.2, float(margin_x_ratio))))))
    my = max(min_margin_px, int(round(ph * max(0.01, min(0.2, float(margin_y_ratio))))))
    if pos_pick == "Giữa trên":
        return max(mx, pw // 2 - lw // 2), my
    if pos_pick == "Trái trên":
        return mx, my
    if pos_pick == "Phải trên":
        return max(mx, pw - lw - mx), my
    if pos_pick == "Trái dưới":
        return mx, max(my, ph - lh - my)
    if pos_pick == "Phải dưới":
        return max(mx, pw - lw - mx), max(my, ph - lh - my)
    if pos_pick == "Giữa dưới":
        return max(mx, pw // 2 - lw // 2), max(my, ph - lh - my)
    return mx, my


def compute_logo_overlay_dimensions(
    media: dict[str, Any] | None,
    *,
    canvas_w: int,
    logo_ratio: float,
    min_side: int = 80,
) -> tuple[int, int]:
    """Tính width×height overlay giữ đúng tỉ lệ ảnh (tránh méo / khung 0)."""
    cw = max(1, int(canvas_w))
    ratio = max(0.02, min(0.6, float(logo_ratio)))
    logo_w = max(min_side, int(cw * ratio))
    mw = int((media or {}).get("width") or 0)
    mh = int((media or {}).get("height") or 0)
    if mw > 0 and mh > 0:
        logo_h = max(2, int(round(logo_w * mh / mw)))
    else:
        logo_h = logo_w
    return max(2, logo_w), max(2, logo_h)


def validate_overlay_clips(
    project: dict[str, Any],
    *,
    media_resolver: Any | None = None,
) -> list[str]:
    """Lỗi tiếng Việt nếu clip overlay/logo thiếu media hoặc file không đọc được."""
    errors: list[str] = []
    media_by_id = {
        str(m.get("id") or ""): m
        for m in (project.get("media") or [])
        if isinstance(m, dict) and m.get("id")
    }

    def _resolve(m: dict[str, Any]) -> Path | None:
        if media_resolver is not None and hasattr(media_resolver, "resolve_media_path_on_disk"):
            return media_resolver.resolve_media_path_on_disk(m)
        return resolve_media_file_path(m)

    for tr in project.get("tracks") or []:
        if not isinstance(tr, dict) or str(tr.get("type") or "") != "overlay":
            continue
        for cl in tr.get("clips") or []:
            if not isinstance(cl, dict) or str(cl.get("type") or "") != "image":
                continue
            cid = str(cl.get("id") or "?")
            mid = str(cl.get("media_id") or "").strip()
            if not mid:
                errors.append(f"logo {cid}: thiếu media_id.")
                continue
            media = media_by_id.get(mid)
            if not media:
                errors.append(f"logo {cid}: media_id={mid} không có trong project.")
                continue
            if str(media.get("type") or "") != "image":
                errors.append(f"logo {cid}: media {mid} không phải ảnh (type={media.get('type')}).")
                continue
            pth = _resolve(media)
            if not pth or not pth.is_file():
                errors.append(
                    f"logo {cid}: không đọc được file ảnh "
                    f"(kiểm tra «Thêm logo / ảnh» và đường dẫn trên máy này)."
                )
                continue
            ow = int(cl.get("width") or 0)
            oh = int(cl.get("height") or 0)
            if ow < 2 or oh < 2:
                errors.append(f"logo {cid}: kích thước overlay quá nhỏ ({ow}×{oh}).")
    return errors
