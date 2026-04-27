from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class BaseVideoAIProvider(ABC):
    @abstractmethod
    def submit_generation(self, request: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def poll_operation(self, operation_id: str) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def download_result(self, operation_id: str, output_dir: str) -> dict[str, Any]:
        raise NotImplementedError

