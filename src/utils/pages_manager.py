"""
Quản lý đích Page/Group (``config/pages.json``) — tách khỏi ``accounts.json``.

Mỗi Page gắn ``account_id`` (owner), không lưu proxy/cookie tại đây.
"""

from __future__ import annotations

import json
import tempfile
import threading
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Optional, TypedDict

from loguru import logger

from src.utils.page_workspace import ensure_page_workspace
from src.utils.paths import project_root


class PageRecord(TypedDict, total=False):
    """Một Page/Group đích để đăng bài (metadata + URL + AI theo page)."""

    id: str
    account_id: str
    page_name: str
    page_url: str
    fb_page_id: str
    use_business_composer: bool
    post_style: str
    schedule_time: str
    page_kind: str
    topic: str
    content_style: str
    post_image_path: str
    status: str
    last_post_at: str
    business_name: str
    business_id: str
    role: str
    source: str


def _default_pages_path() -> Path:
    return project_root() / "config" / "pages.json"


class PagesManager:
    """Đọc/ghi ``pages.json`` — danh sách Page/Group, owner là tài khoản."""

    REQUIRED: tuple[str, ...] = ("id", "account_id", "page_name", "page_url")
    POST_STYLES: tuple[str, ...] = ("post", "image", "video")
    PAGE_KINDS: tuple[str, ...] = ("fanpage", "profile", "group")

    def __init__(self, json_path: Optional[Path | str] = None) -> None:
        self.file_path = Path(json_path).resolve() if json_path else _default_pages_path()
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self._rows_cache: Optional[list[PageRecord]] = None
        self._rows_mtime: Optional[float] = None
        if not self.file_path.is_file():
            self._atomic_write(json.dumps([], ensure_ascii=False, indent=2) + "\n")
            logger.info("Đã tạo file pages rỗng: {}", self.file_path)

    def _atomic_write(self, text: str) -> None:
        d = self.file_path.parent
        fd, tmp = tempfile.mkstemp(prefix="pages_", suffix=".tmp.json", dir=str(d))
        try:
            import os

            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                fh.write(text)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self.file_path)
        except Exception:
            try:
                import os

                os.unlink(tmp)
            except OSError:
                pass
            raise

    def _invalidate_rows_cache(self) -> None:
        self._rows_cache = None
        self._rows_mtime = None

    def reload_from_disk(self) -> list[PageRecord]:
        self._invalidate_rows_cache()
        return self.load_all()

    def _validate(self, row: dict[str, Any]) -> None:
        if not str(row.get("post_style", "")).strip():
            row["post_style"] = "post"
        for k in self.REQUIRED:
            if k not in row or row[k] in (None, ""):
                raise ValueError(f"Thiếu hoặc rỗng trường bắt buộc: {k}")
        url = str(row["page_url"]).strip()
        if not url.startswith("http"):
            raise ValueError("page_url phải bắt đầu bằng http:// hoặc https://")
        ps = str(row["post_style"]).strip().lower()
        if ps not in self.POST_STYLES:
            raise ValueError(f"post_style phải là một trong: {', '.join(self.POST_STYLES)}")
        pk = row.get("page_kind")
        if pk is not None and str(pk).strip():
            pk_s = str(pk).strip().lower()
            if pk_s not in self.PAGE_KINDS:
                raise ValueError(f"page_kind phải là một trong: {', '.join(self.PAGE_KINDS)}")

    def _normalize_fb_page_id(self, row: dict[str, Any]) -> str:
        v = str(row.get("fb_page_id", "") or "").strip()
        if not v:
            return ""
        return v if v.isdigit() else ""

    def validate_record(self, row: dict[str, Any]) -> None:
        """
        Kiểm tra schema một bản ghi (chưa ghi file).

        ``id`` rỗng được phép (form tạo mới); tạm gán id giả để kiểm tra các trường khác.
        ``post_style`` rỗng → gán ``post`` (cấu hình chi tiết ở «Lịch & AI»).
        """
        if not str(row.get("post_style", "")).strip():
            row["post_style"] = "post"
        d = dict(row)
        if not str(d.get("id", "")).strip():
            d["id"] = uuid.uuid4().hex[:12]
        self._validate(d)

    def load_all(self) -> list[PageRecord]:
        if not self.file_path.is_file():
            self._invalidate_rows_cache()
            raise FileNotFoundError(str(self.file_path))
        mtime = self.file_path.stat().st_mtime
        if self._rows_cache is not None and self._rows_mtime == mtime:
            logger.debug("pages load_all: dùng cache ({} dòng)", len(self._rows_cache))
            return list(self._rows_cache)

        raw = json.loads(self.file_path.read_text(encoding="utf-8"))
        if not isinstance(raw, list):
            raise ValueError("pages.json phải là mảng.")
        out: list[PageRecord] = []
        for i, item in enumerate(raw):
            if not isinstance(item, dict):
                raise ValueError(f"Phần tử {i} không phải object.")
            self._validate(item)
            out.append(item)  # type: ignore[arg-type]
        self._rows_cache = out
        self._rows_mtime = mtime
        logger.info("Đã đọc {} page/group từ {}", len(out), self.file_path)
        return list(out)

    def save_all(self, rows: Iterable[PageRecord]) -> None:
        lst = list(rows)
        for r in lst:
            self._validate(r)
        self._atomic_write(json.dumps(lst, ensure_ascii=False, indent=2) + "\n")
        self._rows_cache = list(lst)
        self._rows_mtime = self.file_path.stat().st_mtime
        logger.info("Đã ghi {} page/group vào {}", len(lst), self.file_path)

    def upsert(self, row: PageRecord) -> None:
        d = dict(row)
        if not str(d.get("id", "")).strip():
            d["id"] = uuid.uuid4().hex[:12]
        self._validate(d)
        nid = str(d["id"])
        current = self.load_all()
        fb_id = self._normalize_fb_page_id(d)
        if fb_id:
            for x in current:
                xid = str(x.get("id", "")).strip()
                if xid == nid:
                    continue
                if self._normalize_fb_page_id(dict(x)) == fb_id:
                    raise ValueError(
                        f"Meta Page ID {fb_id} đã tồn tại ở page id={xid!r}. "
                        "Hãy xóa bản ghi cũ trước khi thêm mới.",
                    )
        replaced = False
        new_list: list[PageRecord] = []
        for x in current:
            if str(x.get("id")) == nid:
                new_list.append(d)  # type: ignore[arg-type]
                replaced = True
            else:
                new_list.append(x)
        if not replaced:
            new_list.append(d)  # type: ignore[arg-type]
        self.save_all(new_list)
        try:
            ensure_page_workspace(nid)
        except ValueError as exc:
            logger.warning("Không tạo workspace Page id={}: {}", nid, exc)

    def upsert_many(self, rows: Iterable[PageRecord]) -> dict[str, int]:
        """
        Upsert nhiều bản ghi trong một lần ghi file (tránh ghi ``pages.json`` lặp lại N lần).

        Trả về thống kê ``{"new": x, "updated": y}``.
        """
        incoming: list[PageRecord] = []
        skipped_duplicate_meta_id = 0
        for row in rows:
            d = dict(row)
            if not str(d.get("id", "")).strip():
                d["id"] = uuid.uuid4().hex[:12]
            self._validate(d)
            incoming.append(d)  # type: ignore[arg-type]

        if not incoming:
            return {"new": 0, "updated": 0}

        current = self.load_all()
        by_id: dict[str, PageRecord] = {str(x.get("id")): x for x in current if str(x.get("id"))}
        fb_to_id: dict[str, str] = {}
        for x in current:
            xid = str(x.get("id", "")).strip()
            if not xid:
                continue
            fb = self._normalize_fb_page_id(dict(x))
            if fb and fb not in fb_to_id:
                fb_to_id[fb] = xid
        count_new = 0
        count_updated = 0

        for d in incoming:
            nid = str(d.get("id") or "")
            fb_id = self._normalize_fb_page_id(d)
            if fb_id:
                owner_id = fb_to_id.get(fb_id, "")
                if owner_id and owner_id != nid:
                    skipped_duplicate_meta_id += 1
                    logger.warning(
                        "Bỏ qua upsert_many id={} vì trùng fb_page_id={} với id={}",
                        nid,
                        fb_id,
                        owner_id,
                    )
                    continue
                fb_to_id[fb_id] = nid
            if nid in by_id:
                by_id[nid] = d
                count_updated += 1
            else:
                by_id[nid] = d
                count_new += 1

        new_list = list(by_id.values())
        self.save_all(new_list)

        for d in incoming:
            nid = str(d.get("id") or "").strip()
            if not nid:
                continue
            try:
                ensure_page_workspace(nid)
            except ValueError as exc:
                logger.warning("Không tạo workspace Page id={}: {}", nid, exc)

        return {
            "new": count_new,
            "updated": count_updated,
            "skipped_duplicate_meta_id": skipped_duplicate_meta_id,
        }

    def delete_by_id(self, page_id: str) -> bool:
        cur = self.load_all()
        new_list = [x for x in cur if str(x.get("id")) != page_id]
        if len(new_list) == len(cur):
            return False
        self.save_all(new_list)
        return True

    def delete_by_ids(self, page_ids: Iterable[str]) -> tuple[int, list[str]]:
        """
        Xóa nhiều page theo id trong một lần ghi file.

        Returns:
            ``(removed_count, missing_ids)``.
        """
        ids = [str(x).strip() for x in page_ids if str(x).strip()]
        if not ids:
            return 0, []
        ordered_ids = list(dict.fromkeys(ids))
        remove_set = set(ordered_ids)

        cur = self.load_all()
        existing_ids = {str(x.get("id")).strip() for x in cur if str(x.get("id", "")).strip()}
        new_list = [x for x in cur if str(x.get("id")).strip() not in remove_set]

        removed = len(cur) - len(new_list)
        missing = [pid for pid in ordered_ids if pid not in existing_ids]
        if removed > 0:
            self.save_all(new_list)
        return removed, missing

    def dedupe_by_fb_page_id(self) -> dict[str, int]:
        """
        Dọn trùng theo ``fb_page_id``.

        Giữ lại 1 bản ghi "tốt nhất" cho mỗi Meta Page ID, ưu tiên:
        1) có ``last_post_at`` mới hơn,
        2) có nhiều trường metadata hơn,
        3) fallback theo ``id`` để ổn định.
        """
        cur = self.load_all()
        by_fb: dict[str, list[PageRecord]] = {}
        for r in cur:
            fb = self._normalize_fb_page_id(dict(r))
            if not fb:
                continue
            by_fb.setdefault(fb, []).append(r)

        def _meta_score(rec: PageRecord) -> tuple[str, int, str]:
            last_post = str(rec.get("last_post_at", "") or "").strip()
            meta_fields = (
                "business_name",
                "business_id",
                "role",
                "source",
                "topic",
                "page_kind",
                "schedule_time",
            )
            mcount = sum(1 for k in meta_fields if str(rec.get(k, "") or "").strip())
            rid = str(rec.get("id", "") or "")
            return (last_post, mcount, rid)

        keep_ids: set[str] = set()
        dup_groups = 0
        removed = 0
        for fb, rows in by_fb.items():
            if len(rows) <= 1:
                continue
            dup_groups += 1
            winner = max(rows, key=_meta_score)
            wid = str(winner.get("id", "")).strip()
            if wid:
                keep_ids.add(wid)
            removed += len(rows) - 1
            logger.info(
                "Dedup Meta ID {}: giữ id={} / xóa {} bản trùng",
                fb,
                wid or "—",
                len(rows) - 1,
            )

        if removed == 0:
            return {"groups": 0, "removed": 0}

        new_list: list[PageRecord] = []
        for r in cur:
            rid = str(r.get("id", "")).strip()
            fb = self._normalize_fb_page_id(dict(r))
            if not fb:
                new_list.append(r)
                continue
            # Nhóm không trùng thì giữ nguyên.
            if len(by_fb.get(fb, [])) <= 1:
                new_list.append(r)
                continue
            if rid in keep_ids:
                new_list.append(r)
                continue
            # Bản trùng bị loại.
        self.save_all(new_list)
        return {"groups": dup_groups, "removed": removed}

    def list_for_account(self, account_id: str) -> list[PageRecord]:
        aid = str(account_id).strip()
        return [x for x in self.load_all() if str(x.get("account_id")) == aid]

    def get_by_id(self, page_id: str) -> Optional[PageRecord]:
        pid = str(page_id).strip()
        for x in self.load_all():
            if str(x.get("id")) == pid:
                return x
        return None

    def record_post_outcome(self, page_id: str, *, success: bool) -> None:
        """
        Cập nhật ``status`` và (khi thành công) ``last_post_at`` trên bản ghi Page trong ``pages.json``.
        """
        pid = str(page_id).strip()
        if not pid:
            raise ValueError("page_id rỗng.")
        current = self.load_all()
        new_list: list[PageRecord] = []
        found = False
        for x in current:
            if str(x.get("id")) != pid:
                new_list.append(x)
                continue
            found = True
            d = dict(x)
            if success:
                d["status"] = "success"
                d["last_post_at"] = datetime.now().strftime("%Y-%m-%d %H:%M")
            else:
                d["status"] = "failed"
            new_list.append(d)  # type: ignore[arg-type]
        if not found:
            raise ValueError(f"Không tìm thấy page: {page_id}")
        self.save_all(new_list)
        logger.info("Đã cập nhật outcome page id={} success={}", pid, success)


_default_pages_mgr: Optional[PagesManager] = None
_default_pages_lock = threading.Lock()


def get_default_pages_manager() -> PagesManager:
    """Singleton mặc định cho worker/scheduler sau này."""
    global _default_pages_mgr
    with _default_pages_lock:
        if _default_pages_mgr is None:
            _default_pages_mgr = PagesManager()
        return _default_pages_mgr
