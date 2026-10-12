"""OpenAI trả gói bài, thử lại lỗi tạm, và không tự duyệt."""

from __future__ import annotations

import json

import pytest

from src.ai.providers.openai_text_provider import OpenAITextProvider
from src.services.ai_fleet.content_task import draft_from_provider
from src.services.ai_fleet.gate import schedule_block_reason
from src.services.ai_fleet.store import FleetStore


class _Response:
    def __init__(self, status: int, payload: dict | None = None) -> None:
        self.status_code = status
        self._payload = payload or {}

    def json(self) -> dict:
        return self._payload


def _ok_payload() -> dict:
    body = {
        "title": "Phim hài cổ",
        "body": "Một bộ phim hài cổ điển đang được nhắc lại.",
        "caption": "Xem lại phim hài cổ điển.",
        "hashtags": ["#comedia"],
        "image_prompt": "poster phim hài cổ, không logo giả",
        "language": "pt-BR",
    }
    return {
        "output_text": json.dumps(body),
        "usage": {"input_tokens": 12, "output_tokens": 30},
    }


def test_retries_temporary_error_then_keeps_usage(monkeypatch) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test-secret")
    calls: list[int] = []

    def post(*_args, **_kwargs):
        calls.append(1)
        if len(calls) == 1:
            return _Response(503, {"error": {"message": "sk-test-secret leaked"}})
        return _Response(200, _ok_payload())

    package = OpenAITextProvider(post=post, sleep=lambda _seconds: None).generate_content_package(prompt="viết bài")
    assert len(calls) == 2
    assert package["title"] == "Phim hài cổ"
    assert package["usage"]["input_tokens"] == 12
    assert package["model"]
    assert "sk-test-secret" not in json.dumps(package)


def test_auth_error_does_not_retry_or_echo_the_key(monkeypatch) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test-secret")
    calls: list[int] = []

    def post(*_args, **_kwargs):
        calls.append(1)
        return _Response(401, {"error": "sk-test-secret"})

    with pytest.raises(RuntimeError) as caught:
        OpenAITextProvider(post=post, sleep=lambda _seconds: None).generate_content_package(prompt="viết bài")
    assert len(calls) == 1
    assert "sk-test-secret" not in str(caught.value)
    assert "Không đổi sang provider khác" in str(caught.value)


def test_credit_exhausted_is_not_retried_or_echoed(monkeypatch) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test-secret")
    monkeypatch.setenv("TOOLFB_CHATGPT_ACCOUNT_ID", "acc-web")
    calls: list[int] = []

    def post(*_args, **_kwargs):
        calls.append(1)
        return _Response(429, {"error": {"code": "credit_balance_exhausted", "message": "sk-test-secret no credits"}})

    def web(prompt: str, *, account_id: str) -> str:
        assert account_id == "acc-web"
        assert "viết bài" in prompt
        return json.dumps(
            {
                "title": "Bài web",
                "body": "Nội dung viết trên web.",
                "caption": "Caption web.",
                "hashtags": ["#web"],
                "image_prompt": "ảnh web",
                "language": "vi",
            }
        )

    monkeypatch.setattr("src.ai.image_generation.generate_chatgpt_browser_text", web)
    package = OpenAITextProvider(post=post, sleep=lambda _seconds: None).generate_content_package(prompt="viết bài")
    assert len(calls) == 1
    assert package["title"] == "Bài web"
    assert package["model"] == "chatgpt-web"
    assert "sk-test-secret" not in package["body"]


def test_master_order_is_read_while_writing(tmp_path) -> None:
    store = FleetStore(tmp_path / "fleet.json")
    store.upsert_profile({"page_id": "page-br", "language": "vi"})
    store.add_policy("page-br", {"language": "vi", "requires_source": False, "requires_media": False}, requested_by="user")
    store.set_master_instruction("luôn kết bằng một câu hỏi")
    seen: dict[str, str] = {}

    class Writer:
        provider_name = "openai"

        def generate_content_package(self, *, prompt: str, model: str | None = None) -> dict:
            seen["prompt"] = prompt
            body = _ok_payload()["output_text"]
            return {**json.loads(body), "provider": "openai", "model": "gpt-test", "usage": {"input_tokens": 1, "output_tokens": 1}}

    draft_from_provider(store, page_id="page-br", topic="bữa cơm", provider=Writer())
    assert "luôn kết bằng một câu hỏi" in seen["prompt"]


def test_draft_stays_unapproved_for_the_selected_provider(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test-secret")
    store = FleetStore(tmp_path / "fleet.json")
    store.upsert_profile({"page_id": "page-br", "page_name": "Brazil", "language": "pt-BR", "brand_voice": "ấm"})
    store.add_policy("page-br", {"language": "pt-BR", "requires_source": True, "requires_media": False}, requested_by="user")
    provider = OpenAITextProvider(post=lambda *_a, **_k: _Response(200, _ok_payload()), sleep=lambda _s: None)
    content = draft_from_provider(store, page_id="page-br", topic="phim hài", provider=provider)
    assert content["status"] == "PENDING_REVIEW"
    assert content["provider"] == "openai"
    assert content["usage"]["output_tokens"] == 30
    assert schedule_block_reason(content["content_id"], store)
    saved = store.current_version(content["content_id"])
    assert saved is not None
    assert saved["image_prompt"].startswith("poster")


def test_gpt_image_does_not_send_response_format(monkeypatch) -> None:
    import base64

    from src.ai.providers.openai_image_provider import OpenAIImageProvider

    monkeypatch.setenv("OPENAI_API_KEY", "sk-test-secret")
    monkeypatch.delenv("OPENAI_IMAGE_MODEL", raising=False)
    seen: dict = {}

    class Response:
        status_code = 200

        def json(self) -> dict:
            return {"data": [{"b64_json": base64.b64encode(b"png").decode("ascii")}]}

    def post(url, *, headers, json, timeout):
        seen["url"] = url
        seen["body"] = json
        return Response()

    monkeypatch.setattr("src.ai.providers.openai_image_provider.requests.post", post)
    blobs = OpenAIImageProvider().generate_images(prompt="ô đỏ", number_of_images=1)
    assert blobs == [b"png"]
    assert seen["body"]["model"] == "gpt-image-2"
    assert "response_format" not in seen["body"]


def test_out_of_api_credit_uses_the_chatgpt_browser(monkeypatch) -> None:
    from src.ai.providers.openai_image_provider import OpenAIImageProvider

    monkeypatch.setenv("OPENAI_API_KEY", "sk-test-secret")
    monkeypatch.setenv("TOOLFB_CHATGPT_ACCOUNT_ID", "acc-web")
    seen: dict = {}

    class Response:
        status_code = 429

        def json(self) -> dict:
            return {"error": {"code": "credit_balance_exhausted"}}

    def post(*_args, **_kwargs):
        return Response()

    def web(prompt: str, *, account_id: str, number_of_images: int = 1) -> list[bytes]:
        seen["prompt"] = prompt
        seen["account_id"] = account_id
        seen["count"] = number_of_images
        return [b"png"]

    monkeypatch.setattr("src.ai.providers.openai_image_provider.requests.post", post)
    monkeypatch.setattr("src.ai.image_generation.generate_chatgpt_browser_images", web)
    blobs = OpenAIImageProvider().generate_images(prompt="bữa tối", number_of_images=1)
    assert blobs == [b"png"]
    assert seen["account_id"] == "acc-web"
    assert seen["prompt"] == "bữa tối"


def test_chatgpt_web_image_needs_a_saved_login(tmp_path, monkeypatch) -> None:
    from src.ai.image_generation import generate_chatgpt_browser_images

    monkeypatch.setattr("src.ai.image_generation.chatgpt_profile_dir", lambda _account: tmp_path / "missing")
    with pytest.raises(RuntimeError, match="Chưa đăng nhập ChatGPT trên web"):
        generate_chatgpt_browser_images("một bức ảnh", account_id="acc-web")
