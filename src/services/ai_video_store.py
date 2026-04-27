from __future__ import annotations

import errno
import json
import os
import shutil
import tempfile
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from loguru import logger

from src.services.ai_video_config import load_ai_video_config
from src.utils.media_dedupe import dedupe_output_file_paths
from src.utils.paths import project_root


def _now_iso() -> str:
    return datetime.now().replace(microsecond=0).isoformat()


def _write_text_file_resilient(target: Path, text: str, *, tmp_prefix: str) -> None:
    """
    Ghi file text an toàn trên Windows: temp trong %TEMP%, retry os.replace,
    fallback copy/ghi trực tiếp khi đích bị khóa (WinError 5/32).
    """
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp_root = Path(tempfile.gettempdir())
    fd, tmp_name = tempfile.mkstemp(prefix=tmp_prefix, suffix=".tmp.json", dir=str(tmp_root))
    tmp_path = Path(tmp_name)
    last_err: OSError | None = None
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(text)
            fh.flush()
            os.fsync(fh.fileno())
        for attempt in range(25):
            try:
                os.replace(str(tmp_path), str(target))
                return
            except OSError as e:
                last_err = e
                code = getattr(e, "winerror", None)
                if code in (5, 32) or e.errno in (errno.EACCES, errno.EPERM):
                    time.sleep(0.05 * min(attempt + 1, 12))
                    continue
                break
        try:
            shutil.copyfile(str(tmp_path), str(target))
            return
        except OSError as e2:
            last_err = e2
        target.write_text(text, encoding="utf-8")
    finally:
        try:
            if tmp_path.is_file():
                tmp_path.unlink()
        except Exception:
            pass
    if last_err and not target.is_file():
        raise last_err


def _ai_video_root() -> Path:
    return project_root() / "data" / "ai_video"


def ensure_ai_video_layout() -> dict[str, Path]:
    root = _ai_video_root()
    paths = {
        "root": root,
        "inputs_images": root / "inputs" / "images",
        "inputs_frames": root / "inputs" / "first_last_frames",
        "inputs_refs": root / "inputs" / "reference_images",
        "outputs": root / "outputs",
        "thumbnails": root / "thumbnails",
        "temp": root / "temp",
        "logs": root / "logs",
        "metadata": root / "generated_videos.json",
    }
    for p in paths.values():
        if p.suffix:
            continue
        p.mkdir(parents=True, exist_ok=True)
    if not paths["metadata"].is_file():
        paths["metadata"].parent.mkdir(parents=True, exist_ok=True)
        paths["metadata"].write_text("[]\n", encoding="utf-8")
    return paths


def prepared_prompt_preview_path() -> Path:
    """Đường dẫn file lưu prompt preview chưa chạy tạo video."""
    return ensure_ai_video_layout()["temp"] / "prepared_prompt_preview.json"


def save_prepared_prompt_preview(
    *,
    signature: str,
    requests: list[dict[str, Any]],
    form_snapshot: dict[str, Any] | None = None,
) -> None:
    """
    Lưu prompt preview (và signature form) để khôi phục sau khi đóng/mở lại chương trình.
    ``form_snapshot``: bản chụp form (nhân vật, topic, options…) để khôi phục UI đồng bộ với preview.
    Nếu danh sách rỗng thì xóa file.
    """
    if not requests:
        clear_prepared_prompt_preview()
        return
    p = prepared_prompt_preview_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    payload: dict[str, Any] = {
        "version": 1,
        "signature": str(signature or ""),
        "requests": list(requests),
        "saved_at": _now_iso(),
    }
    if isinstance(form_snapshot, dict) and form_snapshot:
        payload["form_snapshot"] = dict(form_snapshot)
    text = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    _write_text_file_resilient(p, text, tmp_prefix="prepared_preview_")


def load_prepared_prompt_preview() -> tuple[str, list[dict[str, Any]], dict[str, Any] | None] | None:
    """Trả về (signature, requests, form_snapshot|None) nếu có dữ liệu hợp lệ; ngược lại None."""
    p = prepared_prompt_preview_path()
    if not p.is_file():
        return None
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(raw, dict):
        return None
    reqs = raw.get("requests")
    if not isinstance(reqs, list) or not reqs:
        return None
    out: list[dict[str, Any]] = []
    for x in reqs:
        if isinstance(x, dict):
            out.append(dict(x))
    if not out:
        return None
    fs = raw.get("form_snapshot")
    form_snap = dict(fs) if isinstance(fs, dict) else None
    return (str(raw.get("signature", "")), out, form_snap)


def clear_prepared_prompt_preview() -> None:
    """Xóa file prompt preview đã lưu."""
    p = prepared_prompt_preview_path()
    try:
        if p.is_file():
            p.unlink()
    except Exception:
        pass


def ai_video_projects_dir() -> Path:
    """Thư mục lưu file JSON từng dự án AI Video (snapshot form + preview + job ids)."""
    d = ensure_ai_video_layout()["root"] / "projects"
    d.mkdir(parents=True, exist_ok=True)
    return d


def ai_video_project_output_dir(project_id: str) -> Path:
    """Trả thư mục output riêng cho từng dự án ``outputs/{project_id}``."""
    pid = str(project_id or "").strip()
    base = ensure_ai_video_layout()["outputs"]
    if not pid:
        base.mkdir(parents=True, exist_ok=True)
        return base
    out = base / pid
    out.mkdir(parents=True, exist_ok=True)
    return out


def save_ai_video_project_file(body: dict[str, Any]) -> None:
    """Ghi/đè file ``projects/{project_id}.json`` (atomic qua _write_text_file_resilient)."""
    pid = str(body.get("project_id", "") or "").strip()
    if not pid:
        return
    path = ai_video_projects_dir() / f"{pid}.json"
    merged = dict(body)
    prev_created = ""
    if path.is_file():
        try:
            old = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(old, dict):
                prev_created = str(old.get("created_at", "") or "").strip()
        except Exception:
            pass
    if not str(merged.get("created_at", "") or "").strip():
        merged["created_at"] = prev_created or _now_iso()
    merged["updated_at"] = _now_iso()
    text = json.dumps(merged, ensure_ascii=False, indent=2) + "\n"
    _write_text_file_resilient(path, text, tmp_prefix="ai_video_proj_")


def load_ai_video_project_file(project_id: str) -> dict[str, Any] | None:
    """Đọc metadata dự án; None nếu không có file."""
    pid = str(project_id or "").strip()
    if not pid:
        return None
    path = ai_video_projects_dir() / f"{pid}.json"
    if not path.is_file():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return dict(raw) if isinstance(raw, dict) else None


def list_ai_video_project_summaries() -> list[dict[str, Any]]:
    """Danh sách dự án (mới nhất trước) để hiển thị cửa sổ chọn."""
    out: list[dict[str, Any]] = []
    root = ai_video_projects_dir()
    if not root.is_dir():
        return out
    files = sorted(root.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    for path in files:
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(raw, dict):
            continue
        pid = str(raw.get("project_id", "") or path.stem).strip()
        if not pid:
            continue
        out.append(
            {
                "project_id": pid,
                "project_name": str(raw.get("project_name", "") or "").strip() or "Dự án",
                "updated_at": str(raw.get("updated_at", "") or "").strip(),
                "created_at": str(raw.get("created_at", "") or "").strip(),
                "video_job_count": len(raw.get("video_job_ids") or []) if isinstance(raw.get("video_job_ids"), list) else 0,
            }
        )
    return out


def delete_ai_video_project_file(project_id: str) -> bool:
    """Xóa file dự án. Trả True nếu đã xóa được file."""
    pid = str(project_id or "").strip()
    if not pid:
        return False
    path = ai_video_projects_dir() / f"{pid}.json"
    try:
        if path.is_file():
            path.unlink()
            return True
    except Exception:
        pass
    return False


def delete_ai_video_project_output_dir(project_id: str) -> bool:
    """Xóa thư mục output riêng của dự án ``outputs/{project_id}``."""
    pid = str(project_id or "").strip()
    if not pid:
        return False
    path = ensure_ai_video_layout()["outputs"] / pid
    try:
        if path.is_dir():
            shutil.rmtree(path, ignore_errors=False)
            return True
    except Exception:
        pass
    return False


class AIVideoStore:
    def __init__(self) -> None:
        self._paths = ensure_ai_video_layout()

    @property
    def metadata_path(self) -> Path:
        return self._paths["metadata"]

    def load_all(self) -> list[dict[str, Any]]:
        p = self.metadata_path
        if not p.is_file():
            return []
        try:
            raw = json.loads(p.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError, UnicodeError) as exc:
            logger.warning("Không đọc được AI video metadata ({}): {} — trả danh sách rỗng.", p, exc)
            return []
        if not isinstance(raw, list):
            return []
        return [x for x in raw if isinstance(x, dict)]

    def save_all(self, rows: list[dict[str, Any]]) -> None:
        p = self.metadata_path
        text = json.dumps(rows, ensure_ascii=False, indent=2) + "\n"
        _write_text_file_resilient(p, text, tmp_prefix="ai_video_meta_")

    def create_record(self, request: dict[str, Any]) -> dict[str, Any]:
        cfg = load_ai_video_config()
        rows = self.load_all()
        vid = f"vidgen_{uuid.uuid4().hex[:10]}"
        proj_id = str(request.get("project_id", "") or "").strip()
        proj_name = str(request.get("project_name", "") or "").strip()
        rec = {
            "id": vid,
            "project_id": proj_id,
            "project_name": proj_name,
            "provider": str(request.get("provider") or cfg.get("default_provider", "gemini")).strip().lower(),
            "model": str(request.get("model") or "").strip(),
            "mode": str(request.get("mode") or "").strip(),
            "prompt": str(request.get("prompt") or "").strip(),
            "idea": str(request.get("idea") or "").strip(),
            "topic": str(request.get("topic") or "").strip(),
            "goal": str(request.get("goal") or "").strip(),
            "topic_goal": dict(request.get("topic_goal") or {}),
            "language": str(request.get("language") or "").strip(),
            "visual_style": str(request.get("visual_style") or "").strip(),
            "status": "draft",
            "operation_id": "",
            "input_assets": dict(request.get("input_assets") or {}),
            "options": dict(request.get("options") or {}),
            "character_profile_id": str(request.get("character_profile_id") or "").strip(),
            "character_profile": dict(request.get("character_profile") or {}),
            "scene_plan": dict(request.get("scene_plan") or {}),
            "analysis": dict(request.get("analysis") or {}),
            "characters": list(request.get("characters") or []),
            "environments": list(request.get("environments") or []),
            "scenes": list(request.get("scenes") or []),
            "video_map": dict(request.get("video_map") or {}),
            "final_prompt": str(request.get("final_prompt") or "").strip(),
            "output_files": [],
            "thumbnail_path": "",
            "created_at": _now_iso(),
            "started_at": "",
            "completed_at": "",
            "error_message": "",
        }
        rows.append(rec)
        self.save_all(rows)
        return rec

    def update_record(self, video_id: str, patch: dict[str, Any]) -> dict[str, Any]:
        rows = self.load_all()
        for i, row in enumerate(rows):
            if str(row.get("id", "")) == video_id:
                row = dict(row)
                patch2 = dict(patch)
                if "output_files" in patch2:
                    raw = patch2.get("output_files")
                    if isinstance(raw, list):
                        try:
                            patch2["output_files"] = dedupe_output_file_paths(
                                [str(x) for x in raw], delete_duplicate_files=True
                            )
                        except Exception as exc:  # noqa: BLE001
                            logger.warning("dedupe output_files bỏ qua (video_id={}): {}", video_id, exc)
                row.update(patch2)
                rows[i] = row
                self.save_all(rows)
                return row
        raise ValueError(f"Không tìm thấy video_id={video_id}")

    def compact_duplicate_output_files(self) -> int:
        """
        Quét metadata: gỡ ``output_files`` trùng nội dung (xóa file dư, giữ một path mỗi fingerprint).
        Trả về số bản ghi đã chỉnh.
        """
        try:
            rows = self.load_all()
            if not rows:
                return 0
            changed = 0
            new_rows: list[dict[str, Any]] = []
            for row in rows:
                r = dict(row)
                ofs = r.get("output_files")
                if not isinstance(ofs, list) or len(ofs) < 2:
                    new_rows.append(r)
                    continue
                before = [str(x) for x in ofs]
                try:
                    ded = dedupe_output_file_paths(before, delete_duplicate_files=True)
                except Exception as exc:  # noqa: BLE001
                    logger.warning("compact: bỏ qua một bản ghi (id={}): {}", r.get("id"), exc)
                    new_rows.append(r)
                    continue
                if ded != before:
                    r["output_files"] = ded
                    changed += 1
                new_rows.append(r)
            if changed:
                self.save_all(new_rows)
            return changed
        except Exception as exc:  # noqa: BLE001
            logger.warning("compact_duplicate_output_files thất bại: {}", exc)
            return 0

    def delete_records_for_project(self, project_id: str) -> int:
        """Xóa mọi record video có ``project_id`` trùng; trả số bản ghi đã xóa."""
        pid = str(project_id or "").strip()
        if not pid:
            return 0
        rows = self.load_all()
        kept = [r for r in rows if str(r.get("project_id", "") or "").strip() != pid]
        removed = len(rows) - len(kept)
        if removed:
            self.save_all(kept)
        return removed


# Ghi file text an toàn (GUI export bundle, v.v.)
write_resilient_text_file = _write_text_file_resilient

