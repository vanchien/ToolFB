"""Check Live đọc trang hồ sơ trên trình duyệt đã đăng nhập. Không mở Facebook."""

from __future__ import annotations

from src.services.facebook_live_check import check_profiles_on_page, classify_rendered_profile


class _Page:
    def __init__(
        self,
        home: tuple[str, str],
        profiles: dict[str, tuple[str, str, str]],
        later: dict[str, list[str]] | None = None,
    ) -> None:
        self.home = home
        self.profiles = profiles
        self.later = {uid: list(parts) for uid, parts in (later or {}).items()}
        self.url = ""
        self._uid = ""
        self._title = ""
        self._text = ""

    def goto(self, url: str, **_kwargs: object) -> None:
        if "profile.php?id=" in url:
            uid = url.split("profile.php?id=", 1)[1].split("&", 1)[0]
            final, title, text = self.profiles[uid]
            self.url = final
            self._uid = uid
            self._title = title
            self._text = text
            return
        self.url, self._title = self.home
        self._uid = ""
        self._text = "Bảng feed"

    def title(self) -> str:
        return self._title

    def inner_text(self, _selector: str) -> str:
        queued = self.later.get(self._uid) or []
        if queued:
            return queued.pop(0)
        return self._text

    def wait_for_timeout(self, _ms: int) -> None:
        return None


def test_deleted_post_on_a_visible_profile_stays_live() -> None:
    status, detail = classify_rendered_profile(
        "Chung Chiến",
        "Chung Chiến 1,3K người theo dõi Bài viết Giới thiệu đã xóa nội dung",
    )
    assert status == "login_ok"
    assert "Chung Chiến" in detail


def test_lock_flash_then_profile_is_live() -> None:
    page = _Page(
        ("https://www.facebook.com/", "Facebook"),
        {
            "333": (
                "https://www.facebook.com/profile.php?id=333",
                "Chung Chiến",
                "1,3K người theo dõi Bài viết Giới thiệu",
            )
        },
        later={"333": ["Bạn hiện không xem được nội dung này"]},
    )
    found: list[str] = []
    code = check_profiles_on_page(
        page,
        ["333"],
        on_result=lambda _uid, status, _detail: found.append(status),
        sleep=lambda _seconds: None,
    )
    assert code == ""
    assert found == ["login_ok"]


def test_name_alone_is_not_live_until_the_profile_shows() -> None:
    status, _detail = classify_rendered_profile("Nguyen Van A", "Facebook")
    assert status == "error"


def test_rendered_lock_page_is_die_and_profile_is_live() -> None:
    status, detail = classify_rendered_profile(
        "Nguyen Van A",
        "Bạn hiện không xem được nội dung này. Lỗi này thường do chủ sở hữu chỉ chia sẻ nội dung với một nhóm nhỏ.",
    )
    assert status == "login_failed"
    assert detail == "UID không còn hoạt động"
    live, live_detail = classify_rendered_profile(
        "Chung Chiến",
        "Chung Chiến 1,3K người theo dõi · 414 đang theo dõi Bài viết Giới thiệu",
    )
    assert live == "login_ok"
    assert "Chung Chiến" in live_detail


def test_logged_in_browser_checks_each_uid_and_stops_when_logged_out() -> None:
    page = _Page(
        ("https://www.facebook.com/", "Facebook"),
        {
            "111": (
                "https://www.facebook.com/profile.php?id=111",
                "Nguyen Van A",
                "Bạn hiện không xem được nội dung này",
            ),
            "222": (
                "https://www.facebook.com/profile.php?id=222",
                "Chung Chiến",
                "1,3K người theo dõi Bài viết Giới thiệu",
            ),
        },
    )
    found: list[tuple[str, str]] = []
    code = check_profiles_on_page(
        page,
        ["111", "222"],
        on_result=lambda uid, status, _detail: found.append((uid, status)),
        sleep=lambda _seconds: None,
    )
    assert code == ""
    assert found == [("111", "login_failed"), ("222", "login_ok")]

    logged_out = _Page(("https://www.facebook.com/login.php", "Đăng nhập"), {})
    assert check_profiles_on_page(logged_out, ["111"], on_result=lambda *_args: None, sleep=lambda _s: None) == "session"


def test_waits_for_the_lock_page_instead_of_the_loading_name() -> None:
    page = _Page(
        ("https://www.facebook.com/", "Facebook"),
        {
            "111": (
                "https://www.facebook.com/profile.php?id=111",
                "Nguyen Van A",
                "Bạn hiện không xem được nội dung này",
            )
        },
        later={"111": ["Nguyen Van A"]},
    )
    found: list[str] = []
    code = check_profiles_on_page(
        page,
        ["111"],
        on_result=lambda _uid, status, _detail: found.append(status),
        sleep=lambda _seconds: None,
    )
    assert code == ""
    assert found == ["login_failed"]
