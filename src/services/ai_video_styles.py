from __future__ import annotations

from typing import Any

from src.services.ai_styles_registry import load_style_registry, save_style_registry, style_items


def _video_style_note_vi(row: dict[str, Any]) -> str:
    sid = str(row.get("id", "")).strip()
    explicit = str(row.get("description_vi", "")).strip()
    if explicit:
        return explicit
    notes: dict[str, str] = {
        "cinematic_story": "Kể chuyện điện ảnh, chuyển cảnh mượt, cảm xúc rõ.",
        "movie_trailer": "Nhịp trailer kịch tính, tăng cao trào nhanh.",
        "short_film": "Phong cách phim ngắn, tập trung narrative.",
        "dramatic_scene": "Cảnh cảm xúc mạnh, tương phản ánh sáng rõ.",
        "viral_reel": "Tối ưu video ngắn bắt trend, hook nhanh 1-2 giây.",
        "tiktok_trend": "Phong cách trend TikTok, nhịp nhanh và gần gũi.",
        "ugc_video": "Kiểu UGC tự nhiên như người dùng quay thật.",
        "influencer_vlog": "Vlog cá nhân, cảm giác nói chuyện trực tiếp.",
        "product_showcase": "Trưng bày sản phẩm rõ chi tiết, nền sạch.",
        "product_demo": "Demo công dụng theo từng bước dễ hiểu.",
        "beauty_commercial": "Quảng cáo beauty, tông da đẹp, ánh sáng mềm.",
        "luxury_brand": "Thương hiệu cao cấp, khung hình sang trọng.",
        "food_commercial": "Quảng cáo món ăn, nhấn texture hấp dẫn.",
        "explainer_video": "Giải thích ngắn gọn, trực quan, dễ theo dõi.",
        "tutorial_video": "Hướng dẫn từng bước rõ ràng.",
        "educational": "Nội dung giáo dục có cấu trúc.",
        "comedy_sketch": "Tiểu phẩm hài, nhịp diễn vui và biểu cảm.",
        "drama_scene": "Chính kịch, tập trung diễn xuất cảm xúc.",
        "romantic_scene": "Lãng mạn, ánh sáng mềm và kết nối cảm xúc.",
        "horror_scene": "Kinh dị, ánh sáng tối và không khí căng thẳng.",
        "ai_surreal": "Siêu thực AI, hình ảnh mơ ảo sáng tạo.",
        "fantasy_world": "Thế giới fantasy phép thuật, cinematic.",
        "sci_fi": "Khoa học viễn tưởng, bối cảnh tương lai.",
        "cyberpunk": "Cyberpunk neon, tương phản mạnh.",
        "alien_world": "Hành tinh lạ, cảnh quan ngoài hành tinh.",
        "extraterrestrial": "Sinh vật ngoài hành tinh, diện mạo khác thường.",
        "alien_contact": "Bối cảnh first contact đầy hồi hộp.",
        "mysterious_cave": "Hang động bí ẩn, tinh thể phát sáng.",
        "crystal_cave": "Hang tinh thể, hiệu ứng phản chiếu đẹp.",
        "underground_world": "Thế giới ngầm, sinh học phát quang.",
        "kids_exploration": "Khám phá cho trẻ em, màu tươi và an toàn.",
        "kids_storybook": "Tông truyện tranh thiếu nhi, ấm áp.",
        "macro_beauty": "Cận cảnh macro, tôn chi tiết bề mặt.",
        "slow_detail_reveal": "Lộ diện chi tiết chậm, chuyển động mượt.",
        "nature_exploration": "Khám phá thiên nhiên, ánh sáng tự nhiên.",
        "micro_world": "Góc nhìn thế giới tí hon, macro realism.",
        "3d_pixar": "3D kiểu Pixar, nhân vật biểu cảm.",
        "3d_disney": "3D kiểu Disney, màu sắc tươi sáng.",
        "3d_cartoon": "Hoạt hình 3D vui tươi, màu nổi bật.",
    }
    if sid in notes:
        return notes[sid]
    name = str(row.get("name", "")).strip()
    category = str(row.get("category", "")).strip()
    if name and category:
        return f"Phong cách {name} (nhóm {category})."
    if name:
        return f"Phong cách {name}."
    return ""


def default_video_styles() -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for row in style_items("video_styles"):
        name = str(row.get("name", "")).strip()
        prompt = str(row.get("prompt_addon", "")).strip()
        if name and prompt:
            note_vi = _video_style_note_vi(dict(row))
            item: dict[str, str] = {"name": name, "prompt": prompt}
            if note_vi:
                item["note_vi"] = note_vi
            sid = str(row.get("id", "")).strip()
            if sid:
                item["id"] = sid
            out.append(item)
    return out or [{"name": "Cinematic Story", "prompt": "cinematic storytelling video, smooth transitions"}]


def load_video_styles() -> list[dict[str, str]]:
    return default_video_styles()


def save_video_styles(styles: list[dict[str, Any]]) -> None:
    normalized: list[dict[str, str]] = []
    for row in styles:
        if not isinstance(row, dict):
            continue
        name = str(row.get("name", "")).strip()
        prompt = str(row.get("prompt", "")).strip()
        if not name or not prompt:
            continue
        sid = (
            str(row.get("id", "")).strip()
            or name.lower().replace(" ", "_").replace("-", "_")
        )
        item: dict[str, str] = {"id": sid, "name": name, "prompt_addon": prompt}
        note_vi = str(row.get("note_vi", "")).strip()
        if note_vi:
            item["description_vi"] = note_vi
        normalized.append(item)
    reg = load_style_registry()
    reg["video_styles"] = normalized
    save_style_registry(reg)
