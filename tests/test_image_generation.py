"""Prompt builder Imagen — không gọi API."""

from src.ai.image_generation import build_imagen_prompt_from_post


def test_build_imagen_prompt_includes_title_and_excerpt() -> None:
    p = build_imagen_prompt_from_post(
        title="5 sai lầm chăm sóc da",
        body="Đoạn mở bài…\nChi tiết quan trọng.",
        image_style="minimal pastel",
    )
    assert "5 sai lầm" in p
    assert "minimal pastel" in p
    assert "Đoạn mở bài" in p
