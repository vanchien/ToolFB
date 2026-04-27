from __future__ import annotations

import uuid


def build_post_json_prompt(*, topic: str, style: str, language: str) -> str:
    seed = uuid.uuid4().hex[:12]
    return (
        "Bạn là chuyên gia nội dung mạng xã hội. "
        f"Viết bài theo ngôn ngữ: {language}. "
        f"Phong cách: {style}. "
        f"Topic: {topic}. "
        f"Tính đa dạng theo seed: {seed}. "
        'Trả về JSON hợp lệ với đúng 2 key chuỗi: "body", "image_alt". '
        "Không markdown, không giải thích."
    )


def build_topics_prompt(*, idea: str, count: int, goal: str = "", length_hint: str = "") -> str:
    return (
        "Bạn là biên tập nội dung social media. "
        f"Ý tưởng tổng: {idea}. "
        f"Mục tiêu: {goal or 'tổng quát'}. "
        f"Độ dài mong muốn: {length_hint or 'trung bình'}. "
        f"Hãy đề xuất đúng {count} chủ đề ngắn, đa dạng, không trùng lặp. "
        f'Trả về duy nhất JSON: {{"topics": ["...", "..."]}} đúng {count} phần tử.'
    )


def build_hashtags_prompt(*, title: str, body: str, language: str, count: int) -> str:
    return (
        f"Tạo đúng {count} hashtag Facebook liên quan nội dung sau. "
        f"Ngôn ngữ: {language}. "
        "Trả về danh sách hashtag cách nhau bằng dấu phẩy, không giải thích.\n\n"
        f"Title: {title}\nBody: {body}"
    )


def build_title_prompt(*, topic: str, language: str) -> str:
    return (
        f"Viết 1 tiêu đề ngắn 6-12 từ bằng ngôn ngữ {language} cho nội dung sau. "
        "Không hashtag, không emoji, không giải thích.\n\n"
        f"{topic}"
    )


def build_cta_prompt(*, topic: str, language: str) -> str:
    return (
        f"Viết 1 CTA ngắn 1 câu bằng ngôn ngữ {language}, phù hợp nội dung sau. "
        "Không markdown, không giải thích.\n\n"
        f"{topic}"
    )


def build_image_prompt_text_prompt(*, title: str, body: str, language: str) -> str:
    return (
        "Create one concise image-generation prompt for social media illustration. "
        f"Output language: {language}. Return plain text only.\n\n"
        f"Title: {title}\nBody: {body}"
    )
