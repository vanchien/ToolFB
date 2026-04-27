"""app_secrets — nhiều Gemini key + nạp env."""

from __future__ import annotations

import json
import os

import pytest

from src.utils import app_secrets as sec


def test_apply_saved_from_legacy_single_key(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    p = tmp_path / "app_secrets.json"
    p.write_text(json.dumps({"gemini_api_key": "test-key-xyz"}), encoding="utf-8")
    monkeypatch.setattr(sec, "app_secrets_path", lambda: p)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    sec.apply_saved_gemini_key_to_environ()
    assert os.environ.get("GEMINI_API_KEY") == "test-key-xyz"
    body = json.loads(p.read_text(encoding="utf-8"))
    assert "gemini_api_key" not in body
    assert len(body.get("gemini_keys", [])) == 1


def test_apply_does_not_override_existing_env(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    p = tmp_path / "app_secrets.json"
    p.write_text(
        json.dumps(
            {
                "gemini_keys": [{"id": "a", "label": "A", "key": "from-file"}],
                "gemini_active_id": "a",
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(sec, "app_secrets_path", lambda: p)
    monkeypatch.setenv("GEMINI_API_KEY", "from-env")
    sec.apply_saved_gemini_key_to_environ()
    assert os.environ.get("GEMINI_API_KEY") == "from-env"


def test_apply_uses_active_id_when_multiple(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    p = tmp_path / "app_secrets.json"
    p.write_text(
        json.dumps(
            {
                "gemini_keys": [
                    {"id": "a", "label": "A", "key": "first"},
                    {"id": "b", "label": "B", "key": "second"},
                ],
                "gemini_active_id": "b",
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(sec, "app_secrets_path", lambda: p)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    sec.apply_saved_gemini_key_to_environ()
    assert os.environ.get("GEMINI_API_KEY") == "second"


def test_clear_all_removes_env_when_value_was_saved(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    p = tmp_path / "app_secrets.json"
    p.write_text(
        json.dumps(
            {
                "gemini_keys": [{"id": "a", "label": "A", "key": "same"}],
                "gemini_active_id": "a",
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(sec, "app_secrets_path", lambda: p)
    monkeypatch.setenv("GEMINI_API_KEY", "same")
    sec.clear_saved_gemini_key_and_sync_environ()
    body = json.loads(p.read_text(encoding="utf-8"))
    assert body.get("gemini_keys") == []
    assert "GEMINI_API_KEY" not in os.environ


def test_clear_all_keeps_unrelated_env(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    p = tmp_path / "app_secrets.json"
    p.write_text(
        json.dumps(
            {
                "gemini_keys": [{"id": "a", "label": "A", "key": "file-only"}],
                "gemini_active_id": "a",
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(sec, "app_secrets_path", lambda: p)
    monkeypatch.setenv("GEMINI_API_KEY", "user-override")
    sec.clear_saved_gemini_key_and_sync_environ()
    assert os.environ.get("GEMINI_API_KEY") == "user-override"


def test_corrupt_json_backups_and_rewrites_placeholder(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    p = tmp_path / "app_secrets.json"
    p.write_text(
        '{\n  "gemini_keys": [\n    {\n      "id": "x"\n      "label": "oops"\n    }\n  ]\n}\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(sec, "app_secrets_path", lambda: p)
    assert sec.load_app_secrets() == {}
    bak = p.with_name(p.name + ".corrupt.bak")
    assert bak.is_file()
    fixed = json.loads(p.read_text(encoding="utf-8"))
    assert fixed.get("gemini_keys") == []
    assert fixed.get("gemini_active_id") == ""


def test_add_rejects_duplicate_key(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    p = tmp_path / "app_secrets.json"
    p.write_text(
        json.dumps(
            {
                "gemini_keys": [{"id": "a", "label": "A", "key": "dup"}],
                "gemini_active_id": "a",
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(sec, "app_secrets_path", lambda: p)
    with pytest.raises(ValueError, match="đã có"):
        sec.add_gemini_key_entry("B", "dup")


def test_apply_saved_nanobanana_key_to_environ(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    p = tmp_path / "app_secrets.json"
    p.write_text(
        json.dumps(
            {
                "nanobanana_keys": [{"id": "n1", "label": "NB-1", "key": "nb-key-1"}],
                "nanobanana_active_id": "n1",
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(sec, "app_secrets_path", lambda: p)
    monkeypatch.delenv("NANOBANANA_API_KEY", raising=False)
    sec.apply_saved_nanobanana_key_to_environ()
    assert os.environ.get("NANOBANANA_API_KEY") == "nb-key-1"


def test_apply_saved_nanobanana_runtime_config(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    p = tmp_path / "app_secrets.json"
    p.write_text(
        json.dumps(
            {
                "nanobanana_api_url": "https://x/generate",
                "nanobanana_record_info_url": "https://x/record?taskId={task_id}",
                "nanobanana_callback_url": "https://cb/url",
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(sec, "app_secrets_path", lambda: p)
    monkeypatch.delenv("NANOBANANA_API_URL", raising=False)
    monkeypatch.delenv("NANOBANANA_RECORD_INFO_URL", raising=False)
    monkeypatch.delenv("NANOBANANA_CALLBACK_URL", raising=False)
    sec.apply_saved_nanobanana_config_to_environ()
    assert os.environ.get("NANOBANANA_API_URL") == "https://x/generate"
    assert os.environ.get("NANOBANANA_RECORD_INFO_URL") == "https://x/record?taskId={task_id}"
    assert os.environ.get("NANOBANANA_CALLBACK_URL") == "https://cb/url"
