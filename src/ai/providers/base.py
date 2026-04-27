from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass
class TextResult:
    body: str
    image_alt: str


class TextProvider(Protocol):
    provider_name: str

    def generate_post(self, *, prompt: str, model: str | None = None) -> TextResult: ...
    def generate_text(self, *, prompt: str, model: str | None = None) -> str: ...

    def generate_topics(self, *, prompt: str, count: int, model: str | None = None) -> list[str]: ...

    def generate_hashtags(self, *, prompt: str, model: str | None = None) -> str: ...


class ImageProvider(Protocol):
    provider_name: str

    def generate_images(
        self,
        *,
        prompt: str,
        number_of_images: int,
        model: str | None = None,
    ) -> list[bytes]: ...
