"""
Bí mật cục bộ (Gemini API) — ``config/app_secrets.json``.

- Hỗ trợ **nhiều** key (nhãn + key), một key **mặc định** (``gemini_active_id``).
- Biến môi trường ``GEMINI_API_KEY`` nếu đã đặt trước khi chạy được **ưu tiên** khi khởi động (không ghi đè).
- Định dạng cũ ``gemini_api_key`` (một chuỗi) được tự migrate sang danh sách.
"""

from __future__ import annotations

import json
import os
import tempfile
import uuid
from pathlib import Path
from typing import Any

from loguru import logger

from src.utils.paths import project_root

_SECRETS_NAME = "app_secrets.json"


def app_secrets_path() -> Path:
    return project_root() / "config" / _SECRETS_NAME


def _backup_corrupt_secrets_file(path: Path, raw_text: str) -> Path:
    """Sao lưu nội dung lỗi để sửa tay; trả về đường dẫn file .bak."""
    bak = path.with_name(path.name + ".corrupt.bak")
    try:
        bak.write_text(raw_text, encoding="utf-8")
        logger.warning("Đã sao lưu app_secrets lỗi cú pháp → {}", bak)
    except OSError as exc:
        logger.warning("Không ghi được file bak: {}", exc)
    return bak


def _write_minimal_secrets_placeholder(path: Path) -> None:
    """Ghi JSON rỗng hợp lệ để các lần đọc sau không crash."""
    _atomic_write_json(
        path,
        {"gemini_keys": [], "gemini_active_id": "", "nanobanana_keys": [], "nanobanana_active_id": ""},
    )


def _atomic_write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(data, ensure_ascii=False, indent=2) + "\n"
    d = path.parent
    fd, tmp = tempfile.mkstemp(prefix="app_secrets_", suffix=".tmp.json", dir=str(d))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(text)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def load_app_secrets() -> dict[str, Any]:
    p = app_secrets_path()
    if not p.is_file():
        return {}
    try:
        raw_text = p.read_text(encoding="utf-8")
    except OSError as exc:
        logger.warning("Không đọc được {}: {}", p, exc)
        return {}
    text = raw_text.lstrip("\ufeff").strip()
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        logger.error(
            "app_secrets.json không phải JSON hợp lệ: {} — kiểm tra dấu phẩy / dấu ngoặc. "
            "Đã sao lưu bản lỗi và tạo file mới rỗng; thêm lại key trong tab Cài đặt AI.",
            exc,
        )
        _backup_corrupt_secrets_file(p, raw_text)
        try:
            _write_minimal_secrets_placeholder(p)
        except Exception as wexc:  # noqa: BLE001
            logger.warning("Không ghi được placeholder app_secrets: {}", wexc)
        return {}
    except Exception as exc:  # noqa: BLE001
        logger.warning("Không parse được {}: {}", p, exc)
        return {}
    return data if isinstance(data, dict) else {}


def _new_entry_id() -> str:
    return uuid.uuid4().hex[:12]


def mask_api_key_preview(key: str) -> str:
    """Hiển thị an toàn: vài ký tự đầu/cuối + độ dài."""
    k = str(key).strip()
    if not k:
        return "—"
    if len(k) <= 10:
        return "•" * len(k) + f" ({len(k)})"
    return f"{k[:4]}…{k[-4:]} ({len(k)} ký tự)"


def _normalize_key_pool(
    raw: dict[str, Any],
    *,
    pool_key: str,
    active_key: str,
    legacy_single_key: str | None = None,
    legacy_label: str = "Đã nhập (cũ)",
) -> tuple[list[dict[str, str]], str]:
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    raw_list = raw.get(pool_key)
    if isinstance(raw_list, list):
        for item in raw_list:
            if not isinstance(item, dict):
                continue
            ks = str(item.get("key", "")).strip()
            if not ks:
                continue
            eid = str(item.get("id", "")).strip() or _new_entry_id()
            while eid in seen:
                eid = _new_entry_id()
            seen.add(eid)
            lbl = str(item.get("label", "")).strip() or f"Key {eid[:6]}"
            out.append({"id": eid, "label": lbl, "key": ks})
    legacy = str(raw.get(legacy_single_key or "", "")).strip() if legacy_single_key else ""
    if legacy and not any(e["key"] == legacy for e in out):
        out.append({"id": _new_entry_id(), "label": legacy_label, "key": legacy})
    active = str(raw.get(active_key, "")).strip()
    valid = {e["id"] for e in out}
    if out and active not in valid:
        active = out[0]["id"]
    if not out:
        active = ""
    return out, active


def load_normalized_secrets() -> dict[str, Any]:
    """
    Đọc JSON, chuẩn hóa ``gemini_keys`` + ``gemini_active_id``, migrate ``gemini_api_key`` cũ.
    Ghi lại file khi cần (bỏ khóa cũ / sửa active).
    """
    raw = load_app_secrets()
    keys_out, active = _normalize_key_pool(
        raw,
        pool_key="gemini_keys",
        active_key="gemini_active_id",
        legacy_single_key="gemini_api_key",
        legacy_label="Đã nhập (cũ)",
    )
    nb_keys, nb_active = _normalize_key_pool(
        raw,
        pool_key="nanobanana_keys",
        active_key="nanobanana_active_id",
        legacy_single_key="nanobanana_api_key",
        legacy_label="NanoBanana (cũ)",
    )
    oa_keys, oa_active = _normalize_key_pool(
        raw,
        pool_key="openai_keys",
        active_key="openai_active_id",
        legacy_single_key="openai_api_key",
        legacy_label="OpenAI (cũ)",
    )

    new_blob = {
        k: v
        for k, v in raw.items()
        if k
        not in (
            "gemini_keys",
            "gemini_active_id",
            "gemini_api_key",
            "nanobanana_keys",
            "nanobanana_active_id",
            "nanobanana_api_key",
            "openai_keys",
            "openai_active_id",
            "openai_api_key",
        )
    }
    new_blob["gemini_keys"] = keys_out
    new_blob["gemini_active_id"] = active
    new_blob["nanobanana_keys"] = nb_keys
    new_blob["nanobanana_active_id"] = nb_active
    new_blob["openai_keys"] = oa_keys
    new_blob["openai_active_id"] = oa_active

    old_active = str(raw.get("gemini_active_id", "")).strip()
    active_fixed = bool(keys_out) and old_active != active
    old_nb_active = str(raw.get("nanobanana_active_id", "")).strip()
    nb_active_fixed = bool(nb_keys) and old_nb_active != nb_active
    old_oa_active = str(raw.get("openai_active_id", "")).strip()
    oa_active_fixed = bool(oa_keys) and old_oa_active != oa_active
    needs_write = (
        "gemini_api_key" in raw
        or "nanobanana_api_key" in raw
        or "openai_api_key" in raw
        or not isinstance(raw.get("gemini_keys"), list)
        or not isinstance(raw.get("nanobanana_keys"), list)
        or not isinstance(raw.get("openai_keys"), list)
        or active_fixed
        or nb_active_fixed
        or oa_active_fixed
    )

    if needs_write:
        try:
            _atomic_write_json(app_secrets_path(), new_blob)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Không ghi migrate app_secrets: {}", exc)

    return new_blob


def list_gemini_key_rows_for_ui() -> list[dict[str, Any]]:
    """``id``, ``label``, ``preview``, ``is_active``."""
    data = load_normalized_secrets()
    aid = str(data.get("gemini_active_id", ""))
    rows = []
    for e in data.get("gemini_keys", []):
        rows.append(
            {
                "id": e["id"],
                "label": e["label"],
                "preview": mask_api_key_preview(e["key"]),
                "is_active": e["id"] == aid,
            }
        )
    return rows


def get_active_gemini_api_key() -> str:
    """Key đang chọn trong file (chuỗi thô)."""
    data = load_normalized_secrets()
    aid = str(data.get("gemini_active_id", ""))
    for e in data.get("gemini_keys", []):
        if e["id"] == aid:
            return e["key"]
    if data.get("gemini_keys"):
        return str(data["gemini_keys"][0]["key"])
    return ""


def get_saved_gemini_api_key() -> str:
    """Tương thích tên cũ: trả về key mặc định trong file."""
    return get_active_gemini_api_key()


def add_gemini_key_entry(label: str, api_key: str) -> str:
    """Thêm key; trả về ``id``. Trùng nội dung key → ``ValueError``."""
    api_key = str(api_key).strip()
    if not api_key:
        raise ValueError("API key không được để trống.")
    data = load_normalized_secrets()
    keys: list[dict[str, str]] = list(data.get("gemini_keys", []))
    if any(e["key"] == api_key for e in keys):
        raise ValueError("Key này đã có trong danh sách.")
    nid = _new_entry_id()
    lbl = str(label).strip() or f"Key {nid[:6]}"
    keys.append({"id": nid, "label": lbl, "key": api_key})
    active = str(data.get("gemini_active_id", ""))
    if not active or active not in {e["id"] for e in keys}:
        active = nid
    new_blob = {**data, "gemini_keys": keys, "gemini_active_id": active}
    _atomic_write_json(app_secrets_path(), new_blob)
    logger.info("Đã thêm Gemini key id={} vào app_secrets.", nid)
    return nid


def delete_gemini_key_entry(key_id: str) -> str | None:
    """Xóa một mục; trả về key đã xóa (để đồng bộ env) hoặc ``None``."""
    kid = str(key_id).strip()
    data = load_normalized_secrets()
    keys: list[dict[str, str]] = list(data.get("gemini_keys", []))
    removed: str | None = None
    new_keys: list[dict[str, str]] = []
    for e in keys:
        if e["id"] == kid:
            removed = e["key"]
            continue
        new_keys.append(e)
    active = str(data.get("gemini_active_id", ""))
    if active == kid:
        active = new_keys[0]["id"] if new_keys else ""
    new_blob = {**data, "gemini_keys": new_keys, "gemini_active_id": active}
    _atomic_write_json(app_secrets_path(), new_blob)
    logger.info("Đã xóa Gemini key id={} khỏi app_secrets.", kid)
    return removed


def set_preferred_gemini_key_id(key_id: str) -> None:
    """Đặt key mặc định khi mở app / khi env trống."""
    kid = str(key_id).strip()
    data = load_normalized_secrets()
    if not any(e["id"] == kid for e in data.get("gemini_keys", [])):
        raise ValueError("Không tìm thấy key trong danh sách.")
    new_blob = {**data, "gemini_active_id": kid}
    _atomic_write_json(app_secrets_path(), new_blob)


def apply_gemini_key_to_environ(key_id: str | None = None) -> str:
    """
    Gán ``GEMINI_API_KEY`` cho tiến trình hiện tại.
    ``key_id`` = ``None`` → dùng key mặc định trong file.
    Trả về key đã áp dụng (rỗng nếu không có).
    """
    data = load_normalized_secrets()
    kid = str(key_id).strip() if key_id else str(data.get("gemini_active_id", ""))
    key = ""
    for e in data.get("gemini_keys", []):
        if e["id"] == kid:
            key = e["key"]
            break
    if not key and data.get("gemini_keys"):
        key = data["gemini_keys"][0]["key"]
    if key:
        os.environ["GEMINI_API_KEY"] = key
    return key


def apply_saved_gemini_key_to_environ() -> None:
    """Khởi động: nếu env chưa có key, nạp từ file (key mặc định)."""
    if os.environ.get("GEMINI_API_KEY", "").strip():
        return
    k = apply_gemini_key_to_environ(None)
    if k:
        logger.debug("Đã nạp GEMINI_API_KEY từ app_secrets.json (key mặc định).")


def save_gemini_api_key(key: str) -> None:
    """Tương thích cũ: thêm một key với nhãn mặc định."""
    k = str(key).strip()
    if not k:
        raise ValueError("API key trống.")
    add_gemini_key_entry("Nhanh", k)


def clear_saved_gemini_key_and_sync_environ() -> None:
    """
    Xóa **toàn bộ** key trong file (dùng khi người dùng xác nhận «xóa hết»).
    Gỡ ``GEMINI_API_KEY`` khỏi env nếu trùng một key vừa xóa.
    """
    data = load_normalized_secrets()
    prev_keys = {e["key"] for e in data.get("gemini_keys", [])}
    new_blob = {**data, "gemini_keys": [], "gemini_active_id": ""}
    if "gemini_api_key" in new_blob:
        del new_blob["gemini_api_key"]
    _atomic_write_json(app_secrets_path(), new_blob)
    cur = os.environ.get("GEMINI_API_KEY", "").strip()
    if cur and cur in prev_keys:
        del os.environ["GEMINI_API_KEY"]


def gemini_key_status_lines() -> tuple[str, str]:
    env_set = bool(os.environ.get("GEMINI_API_KEY", "").strip())
    data = load_normalized_secrets()
    n = len(data.get("gemini_keys", []))
    if env_set:
        sess = "Phiên hiện tại: đã có GEMINI_API_KEY (AI có thể gọi Gemini)."
    else:
        sess = "Phiên hiện tại: chưa có key trong môi trường — chọn «Kích hoạt» hoặc thêm key."
    if n == 0:
        fle = f"File {app_secrets_path().name}: chưa lưu key nào."
    else:
        aid = str(data.get("gemini_active_id", ""))
        label = "—"
        for e in data.get("gemini_keys", []):
            if e["id"] == aid:
                label = e["label"]
                break
        fle = f"File: {n} key đã lưu. Mặc định khi mở app: «{label}» ({mask_api_key_preview(get_active_gemini_api_key())})."
    return sess, fle


def list_nanobanana_key_rows_for_ui() -> list[dict[str, Any]]:
    data = load_normalized_secrets()
    aid = str(data.get("nanobanana_active_id", ""))
    out: list[dict[str, Any]] = []
    for e in data.get("nanobanana_keys", []):
        out.append(
            {
                "id": e["id"],
                "label": e["label"],
                "preview": mask_api_key_preview(e["key"]),
                "is_active": e["id"] == aid,
            }
        )
    return out


def _get_active_key_from_pool(data: dict[str, Any], pool_key: str, active_key: str) -> str:
    aid = str(data.get(active_key, ""))
    for e in data.get(pool_key, []):
        if e["id"] == aid:
            return e["key"]
    if data.get(pool_key):
        return str(data[pool_key][0]["key"])
    return ""


def add_nanobanana_key_entry(label: str, api_key: str) -> str:
    k = str(api_key).strip()
    if not k:
        raise ValueError("API key không được để trống.")
    data = load_normalized_secrets()
    keys = list(data.get("nanobanana_keys", []))
    if any(e["key"] == k for e in keys):
        raise ValueError("Key này đã có trong danh sách.")
    nid = _new_entry_id()
    lbl = str(label).strip() or f"Key {nid[:6]}"
    keys.append({"id": nid, "label": lbl, "key": k})
    active = str(data.get("nanobanana_active_id", ""))
    if not active or active not in {e["id"] for e in keys}:
        active = nid
    _atomic_write_json(app_secrets_path(), {**data, "nanobanana_keys": keys, "nanobanana_active_id": active})
    return nid


def set_preferred_nanobanana_key_id(key_id: str) -> None:
    kid = str(key_id).strip()
    data = load_normalized_secrets()
    if not any(e["id"] == kid for e in data.get("nanobanana_keys", [])):
        raise ValueError("Không tìm thấy key NanoBanana.")
    _atomic_write_json(app_secrets_path(), {**data, "nanobanana_active_id": kid})


def delete_nanobanana_key_entry(key_id: str) -> str | None:
    kid = str(key_id).strip()
    data = load_normalized_secrets()
    keys = list(data.get("nanobanana_keys", []))
    removed: str | None = None
    new_keys: list[dict[str, str]] = []
    for e in keys:
        if e["id"] == kid:
            removed = e["key"]
            continue
        new_keys.append(e)
    active = str(data.get("nanobanana_active_id", ""))
    if active == kid:
        active = new_keys[0]["id"] if new_keys else ""
    _atomic_write_json(app_secrets_path(), {**data, "nanobanana_keys": new_keys, "nanobanana_active_id": active})
    return removed


def apply_nanobanana_key_to_environ(key_id: str | None = None) -> str:
    data = load_normalized_secrets()
    kid = str(key_id).strip() if key_id else str(data.get("nanobanana_active_id", ""))
    key = ""
    for e in data.get("nanobanana_keys", []):
        if e["id"] == kid:
            key = e["key"]
            break
    if not key:
        key = _get_active_key_from_pool(data, "nanobanana_keys", "nanobanana_active_id")
    if key:
        os.environ["NANOBANANA_API_KEY"] = key
    return key


def apply_saved_nanobanana_key_to_environ() -> None:
    if os.environ.get("NANOBANANA_API_KEY", "").strip():
        return
    apply_nanobanana_key_to_environ(None)


def clear_saved_nanobanana_keys_and_sync_environ() -> None:
    data = load_normalized_secrets()
    prev = {e["key"] for e in data.get("nanobanana_keys", [])}
    _atomic_write_json(app_secrets_path(), {**data, "nanobanana_keys": [], "nanobanana_active_id": ""})
    cur = os.environ.get("NANOBANANA_API_KEY", "").strip()
    if cur and cur in prev:
        del os.environ["NANOBANANA_API_KEY"]


def nanobanana_key_status_lines() -> tuple[str, str]:
    env_set = bool(os.environ.get("NANOBANANA_API_KEY", "").strip())
    data = load_normalized_secrets()
    n = len(data.get("nanobanana_keys", []))
    sess = (
        "Phiên hiện tại: đã có NANOBANANA_API_KEY (ảnh AI có thể gọi NanoBanana)."
        if env_set
        else "Phiên hiện tại: chưa có key NanoBanana trong môi trường."
    )
    if n == 0:
        fle = f"File {app_secrets_path().name}: chưa lưu key NanoBanana."
    else:
        label = "—"
        aid = str(data.get("nanobanana_active_id", ""))
        for e in data.get("nanobanana_keys", []):
            if e["id"] == aid:
                label = e["label"]
                break
        fle = f"NanoBanana: {n} key đã lưu. Mặc định: «{label}»."
    return sess, fle


def list_openai_key_rows_for_ui() -> list[dict[str, Any]]:
    data = load_normalized_secrets()
    aid = str(data.get("openai_active_id", ""))
    rows: list[dict[str, Any]] = []
    for e in data.get("openai_keys", []):
        rows.append(
            {
                "id": e["id"],
                "label": e["label"],
                "preview": mask_api_key_preview(e["key"]),
                "is_active": e["id"] == aid,
            }
        )
    return rows


def add_openai_key_entry(label: str, api_key: str) -> str:
    k = str(api_key).strip()
    if not k:
        raise ValueError("API key không được để trống.")
    data = load_normalized_secrets()
    keys = list(data.get("openai_keys", []))
    if any(e["key"] == k for e in keys):
        raise ValueError("Key này đã có trong danh sách.")
    nid = _new_entry_id()
    lbl = str(label).strip() or f"Key {nid[:6]}"
    keys.append({"id": nid, "label": lbl, "key": k})
    active = str(data.get("openai_active_id", ""))
    if not active or active not in {e["id"] for e in keys}:
        active = nid
    _atomic_write_json(app_secrets_path(), {**data, "openai_keys": keys, "openai_active_id": active})
    return nid


def set_preferred_openai_key_id(key_id: str) -> None:
    kid = str(key_id).strip()
    data = load_normalized_secrets()
    if not any(e["id"] == kid for e in data.get("openai_keys", [])):
        raise ValueError("Không tìm thấy key OpenAI.")
    _atomic_write_json(app_secrets_path(), {**data, "openai_active_id": kid})


def delete_openai_key_entry(key_id: str) -> str | None:
    kid = str(key_id).strip()
    data = load_normalized_secrets()
    keys = list(data.get("openai_keys", []))
    removed: str | None = None
    new_keys: list[dict[str, str]] = []
    for e in keys:
        if e["id"] == kid:
            removed = e["key"]
            continue
        new_keys.append(e)
    active = str(data.get("openai_active_id", ""))
    if active == kid:
        active = new_keys[0]["id"] if new_keys else ""
    _atomic_write_json(app_secrets_path(), {**data, "openai_keys": new_keys, "openai_active_id": active})
    return removed


def apply_openai_key_to_environ(key_id: str | None = None) -> str:
    data = load_normalized_secrets()
    kid = str(key_id).strip() if key_id else str(data.get("openai_active_id", ""))
    key = ""
    for e in data.get("openai_keys", []):
        if e["id"] == kid:
            key = e["key"]
            break
    if not key:
        key = _get_active_key_from_pool(data, "openai_keys", "openai_active_id")
    if key:
        os.environ["OPENAI_API_KEY"] = key
    return key


def apply_saved_openai_key_to_environ() -> None:
    if os.environ.get("OPENAI_API_KEY", "").strip():
        return
    apply_openai_key_to_environ(None)


def clear_saved_openai_keys_and_sync_environ() -> None:
    data = load_normalized_secrets()
    prev = {e["key"] for e in data.get("openai_keys", [])}
    _atomic_write_json(app_secrets_path(), {**data, "openai_keys": [], "openai_active_id": ""})
    cur = os.environ.get("OPENAI_API_KEY", "").strip()
    if cur and cur in prev:
        del os.environ["OPENAI_API_KEY"]


def openai_key_status_lines() -> tuple[str, str]:
    env_set = bool(os.environ.get("OPENAI_API_KEY", "").strip())
    data = load_normalized_secrets()
    n = len(data.get("openai_keys", []))
    sess = (
        "Phiên hiện tại: đã có OPENAI_API_KEY (AI có thể gọi OpenAI)."
        if env_set
        else "Phiên hiện tại: chưa có key OpenAI trong môi trường."
    )
    if n == 0:
        fle = f"File {app_secrets_path().name}: chưa lưu key OpenAI."
    else:
        label = "—"
        aid = str(data.get("openai_active_id", ""))
        for e in data.get("openai_keys", []):
            if e["id"] == aid:
                label = e["label"]
                break
        fle = f"OpenAI: {n} key đã lưu. Mặc định: «{label}»."
    return sess, fle


def get_nanobanana_runtime_config() -> dict[str, str]:
    """
    Trả về cấu hình endpoint/callback NanoBanana từ file secrets.
    """
    d = load_normalized_secrets()
    return {
        "api_url": str(d.get("nanobanana_api_url", "")).strip(),
        "record_info_url": str(d.get("nanobanana_record_info_url", "")).strip(),
        "callback_url": str(d.get("nanobanana_callback_url", "")).strip(),
        "web_url": str(d.get("nanobanana_web_url", "")).strip(),
        "account_label": str(d.get("nanobanana_account_label", "")).strip(),
        "video_model": str(d.get("nanobanana_video_model", "")).strip(),
        "locked_ui": str(d.get("nanobanana_locked_ui", "")).strip(),
        "enforce_model": str(d.get("nanobanana_enforce_model", "")).strip(),
        "action_delay_ms": str(d.get("nanobanana_action_delay_ms", "")).strip(),
    }


def save_nanobanana_runtime_config(
    *,
    api_url: str,
    record_info_url: str,
    callback_url: str,
    web_url: str = "",
    account_label: str = "",
    video_model: str = "",
    locked_ui: str | None = None,
    enforce_model: str | None = None,
    action_delay_ms: str | None = None,
) -> None:
    d = load_normalized_secrets()
    n = {
        **d,
        "nanobanana_api_url": str(api_url).strip(),
        "nanobanana_record_info_url": str(record_info_url).strip(),
        "nanobanana_callback_url": str(callback_url).strip(),
        "nanobanana_web_url": str(web_url).strip(),
        "nanobanana_account_label": str(account_label).strip(),
        "nanobanana_video_model": str(video_model).strip(),
    }
    if locked_ui is not None:
        n["nanobanana_locked_ui"] = str(locked_ui).strip()
    if enforce_model is not None:
        n["nanobanana_enforce_model"] = str(enforce_model).strip()
    if action_delay_ms is not None:
        n["nanobanana_action_delay_ms"] = str(action_delay_ms).strip()
    _atomic_write_json(app_secrets_path(), n)


def apply_saved_nanobanana_config_to_environ() -> None:
    cfg = get_nanobanana_runtime_config()
    if cfg.get("api_url") and not os.environ.get("NANOBANANA_API_URL", "").strip():
        os.environ["NANOBANANA_API_URL"] = cfg["api_url"]
    if cfg.get("record_info_url") and not os.environ.get("NANOBANANA_RECORD_INFO_URL", "").strip():
        os.environ["NANOBANANA_RECORD_INFO_URL"] = cfg["record_info_url"]
    if cfg.get("callback_url") and not os.environ.get("NANOBANANA_CALLBACK_URL", "").strip():
        os.environ["NANOBANANA_CALLBACK_URL"] = cfg["callback_url"]
    if cfg.get("web_url") and not os.environ.get("NANOBANANA_WEB_URL", "").strip():
        os.environ["NANOBANANA_WEB_URL"] = cfg["web_url"]
    if cfg.get("video_model") and not os.environ.get("GEMINI_VIDEO_MODEL", "").strip():
        os.environ["GEMINI_VIDEO_MODEL"] = cfg["video_model"]
    if cfg.get("locked_ui") and not os.environ.get("NANOBANANA_LOCKED_UI", "").strip():
        os.environ["NANOBANANA_LOCKED_UI"] = cfg["locked_ui"]
    if cfg.get("enforce_model") and not os.environ.get("NANOBANANA_ENFORCE_MODEL", "").strip():
        os.environ["NANOBANANA_ENFORCE_MODEL"] = cfg["enforce_model"]
    if cfg.get("action_delay_ms") and not os.environ.get("NANOBANANA_ACTION_DELAY_MS", "").strip():
        os.environ["NANOBANANA_ACTION_DELAY_MS"] = cfg["action_delay_ms"]
