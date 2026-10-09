"""Cửa sổ tìm nhóm, tham gia và chia sẻ link. Không tải video."""

from __future__ import annotations

import threading
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from typing import Any

from src.gui.ui_responsiveness import schedule_on_main_thread

from src.services.facebook_groups.classify import parse_min_members
from src.services.facebook_groups.engine import GroupEngine
from src.services.facebook_groups.facebook_provider import FacebookGroupProvider
from src.services.facebook_groups.fingerprint import classify_source_url, link_preview, normalize_share_images
from src.services.facebook_groups.store import GroupStore


def open_group_manager_dialog(parent: tk.Misc) -> None:
    """Mở Group Manager trên cửa sổ chính."""
    GroupManagerDialog(parent)


class GroupManagerDialog(tk.Toplevel):
    """Tìm Group theo cấu hình, chọn nhóm để tham gia hoặc chia sẻ URL."""

    def __init__(self, parent: tk.Misc) -> None:
        super().__init__(parent)
        self.title("Nhóm Facebook")
        self.geometry("980x680")
        self.minsize(720, 480)
        self.transient(parent)
        self.store = GroupStore()
        self.engine = GroupEngine(self.store, FacebookGroupProvider())
        self.engine.recover()
        self._build()

    def _build(self) -> None:
        catalog = self.store.catalog()
        top = ttk.Frame(self, padding=8)
        top.pack(fill=tk.X)
        ttk.Label(top, text="Chủ đề").grid(row=0, column=0, sticky="w")
        self.topic = tk.StringVar()
        ttk.Entry(top, textvariable=self.topic, width=28).grid(row=0, column=1, sticky="w")
        ttk.Label(top, text="Từ khóa").grid(row=1, column=0, sticky="w")
        self.keywords = tk.StringVar()
        ttk.Entry(top, textvariable=self.keywords, width=48).grid(row=1, column=1, columnspan=2, sticky="w")
        ttk.Label(top, text="Quốc gia").grid(row=0, column=3, sticky="w", padx=(12, 4))
        self.countries = tk.Listbox(top, selectmode=tk.MULTIPLE, height=4, exportselection=False, width=22)
        for row in catalog.get("countries") or []:
            self.countries.insert(tk.END, f"{row.get('code')}  {row.get('name')}")
        self.countries.grid(row=0, column=4, rowspan=2, sticky="w")
        ttk.Label(top, text="Ngôn ngữ").grid(row=0, column=5, sticky="w", padx=(12, 4))
        self.languages = tk.Listbox(top, selectmode=tk.MULTIPLE, height=4, exportselection=False, width=18)
        for row in catalog.get("languages") or []:
            self.languages.insert(tk.END, f"{row.get('code')}  {row.get('name')}")
        self.languages.grid(row=0, column=6, rowspan=2, sticky="w")

        filters = ttk.Frame(self, padding=(8, 0))
        filters.pack(fill=tk.X)
        self.mem_joined = tk.BooleanVar(value=True)
        self.mem_joinable = tk.BooleanVar(value=True)
        self.mem_approval = tk.BooleanVar(value=False)
        self.mem_unknown = tk.BooleanVar(value=True)
        self.post_yes = tk.BooleanVar(value=True)
        self.post_no = tk.BooleanVar(value=False)
        self.post_unknown = tk.BooleanVar(value=True)
        ttk.Checkbutton(filters, text="Đã tham gia", variable=self.mem_joined).pack(side=tk.LEFT)
        ttk.Checkbutton(filters, text="Tham gia trực tiếp", variable=self.mem_joinable).pack(side=tk.LEFT)
        ttk.Checkbutton(filters, text="Cần duyệt", variable=self.mem_approval).pack(side=tk.LEFT)
        ttk.Checkbutton(filters, text="Chưa rõ", variable=self.mem_unknown).pack(side=tk.LEFT, padx=(0, 12))
        ttk.Checkbutton(filters, text="Được đăng", variable=self.post_yes).pack(side=tk.LEFT)
        ttk.Checkbutton(filters, text="Không được đăng", variable=self.post_no).pack(side=tk.LEFT)
        ttk.Checkbutton(filters, text="Đăng chưa rõ", variable=self.post_unknown).pack(side=tk.LEFT)
        ttk.Label(filters, text="Tối thiểu thành viên").pack(side=tk.LEFT, padx=(12, 4))
        self.min_members = tk.StringVar(value="10000")
        ttk.Entry(filters, textvariable=self.min_members, width=10).pack(side=tk.LEFT)
        self.strict_members = tk.BooleanVar(value=True)
        ttk.Checkbutton(filters, text="Chỉ nhận số đếm rõ", variable=self.strict_members).pack(side=tk.LEFT, padx=(8, 0))

        actions = ttk.Frame(self, padding=8)
        actions.pack(fill=tk.X)
        ttk.Button(actions, text="Tìm nhóm", command=self._search).pack(side=tk.LEFT)
        ttk.Button(actions, text="Lưu hồ sơ tìm", command=self._save_profile).pack(side=tk.LEFT, padx=4)
        self.profile_name = tk.StringVar()
        self.profile_box = ttk.Combobox(actions, textvariable=self.profile_name, width=22, state="readonly")
        self.profile_box.pack(side=tk.LEFT, padx=4)
        ttk.Button(actions, text="Chạy hồ sơ đã lưu", command=self._run_profile).pack(side=tk.LEFT)
        ttk.Button(actions, text="Tạm dừng", command=self._pause_search).pack(side=tk.LEFT, padx=4)
        ttk.Button(actions, text="Tiếp tục", command=self._resume_search).pack(side=tk.LEFT)
        ttk.Button(actions, text="Tham gia nhóm đã chọn", command=self._join_selected).pack(side=tk.LEFT, padx=4)
        self._discovery_job_id = ""
        self._refresh_profiles()
        self.status = tk.StringVar(value=self._summary())
        ttk.Label(actions, textvariable=self.status).pack(side=tk.LEFT, padx=8)

        cols = ("group", "members", "count_status", "country", "lang", "topic", "url")
        self.tree = ttk.Treeview(self, columns=cols, show="headings", selectmode="extended", height=12)
        for key, title, width in (
            ("group", "Nhóm", 240),
            ("members", "Thành viên", 90),
            ("count_status", "Số đếm", 90),
            ("country", "Nước", 70),
            ("lang", "Ngôn ngữ", 80),
            ("topic", "Chủ đề", 90),
            ("url", "Link", 220),
        ):
            self.tree.heading(key, text=title)
            self.tree.column(key, width=width, stretch=True)
        self.tree.pack(fill=tk.BOTH, expand=True, padx=8)

        share = ttk.LabelFrame(self, text="Chia sẻ bài — link Page khác, chữ và ảnh. Không tải video", padding=8)
        share.pack(fill=tk.X, padx=8, pady=8)
        ttk.Label(share, text="Link bài").grid(row=0, column=0, sticky="w")
        self.source_url = tk.StringVar()
        ttk.Entry(share, textvariable=self.source_url, width=64).grid(row=0, column=1, sticky="we")
        ttk.Label(share, text="Nội dung").grid(row=1, column=0, sticky="w")
        self.caption = tk.StringVar()
        ttk.Entry(share, textvariable=self.caption, width=64).grid(row=1, column=1, sticky="we")
        ttk.Label(share, text="Ảnh").grid(row=2, column=0, sticky="w")
        self.image_paths: list[str] = []
        self.image_label = tk.StringVar(value="Chưa chọn ảnh")
        image_row = ttk.Frame(share)
        image_row.grid(row=2, column=1, sticky="w")
        ttk.Button(image_row, text="Chọn ảnh", command=self._pick_images).pack(side=tk.LEFT)
        ttk.Label(image_row, textvariable=self.image_label).pack(side=tk.LEFT, padx=6)
        self.share_destination = tk.StringVar(value="page")
        dest = ttk.Frame(share)
        dest.grid(row=3, column=1, sticky="w")
        ttk.Radiobutton(dest, text="Lên Page của tôi", variable=self.share_destination, value="page").pack(side=tk.LEFT)
        ttk.Radiobutton(dest, text="Vào nhóm đã tham gia", variable=self.share_destination, value="joined").pack(side=tk.LEFT, padx=8)
        ttk.Label(share, text="Account").grid(row=4, column=0, sticky="w")
        self.account_id = tk.StringVar()
        account_ids = self._account_ids()
        self.account_box = ttk.Combobox(share, textvariable=self.account_id, values=account_ids, width=36)
        self.account_box.grid(row=4, column=1, sticky="w")
        self.account_box.bind("<<ComboboxSelected>>", lambda _event: self._fill_pages())
        ttk.Label(share, text="Page").grid(row=5, column=0, sticky="w")
        self.page_label = tk.StringVar()
        self._page_labels: dict[str, str] = {"— Tài khoản —": ""}
        self._page_urls: dict[str, str] = {}
        self.page_box = ttk.Combobox(share, textvariable=self.page_label, width=36, state="readonly")
        self.page_box.grid(row=5, column=1, sticky="w")
        self._fill_pages()
        ttk.Button(share, text="Chia sẻ", command=self._create_share).grid(row=6, column=1, sticky="w", pady=4)
        self.preview = tk.StringVar(value="")
        ttk.Label(share, textvariable=self.preview, wraplength=640).grid(row=7, column=0, columnspan=2, sticky="w")
        share.columnconfigure(1, weight=1)
        self._reload_tree()

    def _filters(self) -> dict[str, Any]:
        return {
            "membership": {
                "joined": self.mem_joined.get(),
                "joinable": self.mem_joinable.get(),
                "approval": self.mem_approval.get(),
                "unknown": self.mem_unknown.get(),
            },
            "posting": {
                "can_post": self.post_yes.get(),
                "cannot_post": self.post_no.get(),
                "unknown": self.post_unknown.get(),
            },
        }

    def _selected_codes(self, box: tk.Listbox) -> list[str]:
        codes = []
        for index in box.curselection():
            text = str(box.get(index))
            codes.append(text.split()[0].strip())
        return codes

    def _account_ids(self) -> list[str]:
        try:
            from src.utils.db_manager import AccountsDatabaseManager

            return [str(row.get("id") or "") for row in AccountsDatabaseManager().load_all() if row.get("id")]
        except Exception:  # noqa: BLE001
            return []

    def _search(self) -> None:
        account_id = self.account_id.get().strip()
        if not account_id:
            messagebox.showwarning("Thiếu account", "Chọn account ở phía dưới trước khi tìm nhóm.", parent=self)
            return
        keywords = [part.strip() for part in self.keywords.get().split(",") if part.strip()]
        if not keywords and not self.topic.get().strip():
            messagebox.showwarning("Thiếu từ khóa", "Nhập chủ đề hoặc từ khóa để quét nhóm.", parent=self)
            return
        raw_minimum = self.min_members.get().strip()
        minimum = None
        if raw_minimum:
            try:
                minimum = parse_min_members(raw_minimum)
            except ValueError:
                messagebox.showwarning("Số thành viên", "Nhập số như 10000, 10k hoặc 10m.", parent=self)
                return
            if minimum <= 0:
                minimum = None
        job = self.store.add_discovery_job(
            {
                "topic": self.topic.get().strip(),
                "keywords": keywords,
                "countries": self._selected_codes(self.countries),
                "languages": self._selected_codes(self.languages),
                "filters": self._filters(),
                "account_id": account_id,
                "min_members": minimum,
                "strict_member_count": bool(self.strict_members.get()),
            }
        )
        self._discovery_job_id = job["id"]
        self.status.set("Đang quét nhóm…")

        def _show(text: str) -> None:
            schedule_on_main_thread(self, lambda: self.status.set(text))

        self.engine.on_progress = _show

        def _work() -> None:
            self.engine.run_discovery(job["id"])
            schedule_on_main_thread(self, self._reload_tree)

        threading.Thread(target=_work, name="group-discovery", daemon=True).start()

    def _save_profile(self) -> None:
        self.store.save_profile(
            {
                "name": self.topic.get().strip() or "Hồ sơ tìm nhóm",
                "topics": [self.topic.get().strip()] if self.topic.get().strip() else [],
                "keywords": [part.strip() for part in self.keywords.get().split(",") if part.strip()],
                "countries": self._selected_codes(self.countries),
                "languages": self._selected_codes(self.languages),
                "schedule": "manual",
                "filters": self._filters(),
            }
        )
        self._refresh_profiles()
        self.status.set("Đã lưu hồ sơ tìm. Chưa tự tham gia nhóm mới.")

    def _selected_page_id(self) -> str:
        return str(self._page_labels.get(self.page_label.get(), "") or "")

    def _fill_pages(self) -> None:
        labels = ["— Tài khoản —"]
        mapping = {"— Tài khoản —": ""}
        urls: dict[str, str] = {}
        try:
            from src.utils.pages_manager import PagesManager

            account_id = self.account_id.get().strip()
            rows = PagesManager().list_for_account(account_id) if account_id else PagesManager().load_all()
            for row in rows:
                name = str(row.get("page_name") or row.get("id") or "Page")
                page_id = str(row.get("fb_page_id") or row.get("id") or "")
                label = f"{name} · {page_id}" if page_id else name
                mapping[label] = page_id
                urls[label] = str(row.get("page_url") or "")
                labels.append(label)
        except Exception:  # noqa: BLE001
            pass
        self._page_labels = mapping
        self._page_urls = urls
        self.page_box["values"] = labels
        if self.page_label.get() not in mapping:
            self.page_label.set(labels[0])

    def _refresh_profiles(self) -> None:
        names = [str(row.get("name") or "") for row in self.store.profiles() if row.get("name")]
        self.profile_box["values"] = names

    def _run_profile(self) -> None:
        name = self.profile_name.get().strip()
        profile = next((row for row in self.store.profiles() if str(row.get("name") or "") == name), None)
        if profile is None:
            messagebox.showwarning("Chưa chọn hồ sơ", "Lưu hồ sơ tìm rồi chọn lại.", parent=self)
            return
        topics = list(profile.get("topics") or [])
        self.topic.set(str(topics[0] if topics else ""))
        self.keywords.set(", ".join(str(item) for item in (profile.get("keywords") or [])))
        self._select_codes(self.countries, list(profile.get("countries") or []))
        self._select_codes(self.languages, list(profile.get("languages") or []))
        self._search()

    def _select_codes(self, box: tk.Listbox, codes: list[str]) -> None:
        wanted = {str(code) for code in codes}
        box.selection_clear(0, tk.END)
        for index in range(box.size()):
            code = str(box.get(index)).split()[0]
            if code in wanted:
                box.selection_set(index)

    def _run_later(self, label: str, work) -> None:
        """Tham gia và chia sẻ chạy nền để cửa sổ không đứng trong lúc chờ cooldown."""
        self.status.set(label)

        def _wrapped() -> None:
            try:
                work()
            finally:
                schedule_on_main_thread(self, self._reload_tree)

        threading.Thread(target=_wrapped, name="group-job", daemon=True).start()

    def _pause_search(self) -> None:
        if not self._discovery_job_id:
            return
        self.engine.pause_discovery(self._discovery_job_id)
        self.status.set("Sẽ dừng trước lượt tìm kế tiếp.")

    def _resume_search(self) -> None:
        job_id = self._discovery_job_id
        if not job_id:
            return
        self._run_later("Đang tiếp tục quét…", lambda: self.engine.resume_discovery(job_id))

    def _join_selected(self) -> None:
        account_id = self.account_id.get().strip()
        if not account_id:
            messagebox.showwarning("Thiếu account", "Chọn account trước khi tham gia nhóm.", parent=self)
            return
        group_ids = [str(iid) for iid in self.tree.selection()]
        if not group_ids:
            messagebox.showwarning("Chưa chọn", "Chọn ít nhất một nhóm.", parent=self)
            return
        page_id = self._selected_page_id()
        created = self.engine.enqueue_joins(account_id=account_id, page_id=page_id, group_ids=group_ids)
        if not created:
            self.status.set("Các nhóm đã chọn đang có job tham gia.")
            return
        self._run_later("Đang tham gia nhóm…", lambda: self.engine.run_join_queue(account_id))

    def _pick_images(self) -> None:
        picked = filedialog.askopenfilenames(
            parent=self,
            title="Chọn ảnh để kèm bài",
            filetypes=[("Ảnh", "*.jpg *.jpeg *.png *.webp *.gif")],
        )
        self.image_paths = [str(path) for path in picked]
        self.image_label.set(f"{len(self.image_paths)} ảnh" if self.image_paths else "Chưa chọn ảnh")

    def _create_share(self) -> None:
        url = self.source_url.get().strip()
        if not url:
            messagebox.showwarning("Thiếu link", "Dán link bài của Page khác. Tool không tải video.", parent=self)
            return
        try:
            images = normalize_share_images(self.image_paths)
        except ValueError as exc:
            messagebox.showwarning("Ảnh", str(exc), parent=self)
            return
        preview = link_preview(url)
        self.preview.set(f"{classify_source_url(url)} · {preview.get('domain') or url} · {len(images)} ảnh")
        destination = self.share_destination.get()
        page_id = self._selected_page_id()
        group_ids = [str(iid) for iid in self.tree.selection()] if destination == "joined" else []
        if destination == "page" and not page_id:
            messagebox.showwarning("Thiếu Page", "Chọn Page của mình để chia sẻ lên đó.", parent=self)
            return
        result = self.engine.create_share_batch(
            account_id=self.account_id.get().strip(),
            page_id=page_id,
            source_url=url,
            text=self.caption.get().strip(),
            group_ids=group_ids,
            image_paths=images,
            destination=destination,
            target_url=self._page_urls.get(self.page_label.get(), ""),
        )
        created = len(result.get("created") or [])
        skipped = len(result.get("skipped") or [])
        duplicates = len(result.get("duplicates") or [])
        self.status.set(f"Đã tạo {created} job chia sẻ. Bỏ qua {skipped}. Trùng {duplicates}.")
        if result.get("batch") and created:
            batch_id = result["batch"]["id"]
            self._run_later("Đang chia sẻ link…", lambda: self.engine.run_share_batch(batch_id))

    def _reload_tree(self) -> None:
        for item in self.tree.get_children():
            self.tree.delete(item)
        rows = self.engine.visible_groups(
            self._filters(),
            account_id=self.account_id.get().strip(),
            page_id=self._selected_page_id(),
        )
        for row in rows:
            group_id = str(row.get("group_id") or "")
            self.tree.insert(
                "",
                tk.END,
                iid=group_id,
                values=(
                    row.get("group_name") or group_id,
                    f"{int(row.get('member_count') or 0):,}" if row.get("member_count") else "—",
                    row.get("member_count_status") or "UNKNOWN",
                    row.get("country") or "—",
                    row.get("language") or "—",
                    "Khớp" if row.get("topic_match") else "—",
                    row.get("group_url") or "—",
                ),
            )
        saved = len(self.store.groups())
        note = ""
        jobs = self.store.discovery_jobs()
        if jobs:
            note = str(jobs[-1].get("note") or jobs[-1].get("error_message") or "")
        if saved and not rows:
            self.status.set(
                f"Đã quét {saved} nhóm. Bật ô “Đăng chưa rõ” để xem, vì trang tìm chưa cho biết nhóm nào được đăng."
            )
            return
        summary = self._summary()
        self.status.set(f"{summary} · {note}" if note else summary)

    def _summary(self) -> str:
        data = self.engine.dashboard()
        return (
            f"Tìm: {data['searches']} · Nhóm: {data['unique_groups']} · "
            f"Đã vào: {data['joined']} · Chờ duyệt: {data['approval_required']} · "
            f"Chưa rõ: {data['unknown']}"
        )
