"""Sinh tên Page và lấy ảnh ngẫu nhiên từ thư mục."""

from __future__ import annotations

import random
from pathlib import Path

from src.services.page_creation.naming import (
    account_display_label,
    assign_random_avatars,
    business_display_label,
    generate_page_names,
    image_files_in_folder,
    name_capacity,
    suffixes_for_topic,
    topic_from_account_name,
)


def _png(path: Path) -> None:
    path.write_bytes(b"\x89PNG\r\n\x1a\n" + b"png")


def test_generate_names_keep_topic_and_stay_unique() -> None:
    names = generate_page_names("Brazil Funny", 8, language="en", rng=random.Random(1))
    assert len(names) == 8
    assert len({name.casefold() for name in names}) == 8
    assert all("brazil funny" in name.casefold() for name in names)


def test_english_language_skips_vietnamese_topic() -> None:
    """Chọn Tiếng Anh + chủ đề Việt (tên account) không được nhúng chữ Việt vào tên."""
    names = generate_page_names("Tài khoản mới", 30, language="en", rng=random.Random(9))
    assert len(names) == 30
    joined = " ".join(names)
    assert "Tài" not in joined
    assert "khoản" not in joined
    assert all(name.isascii() for name in names)
    # Chủ đề tiếng Anh vẫn được giữ khi chọn en.
    with_topic = generate_page_names("Brazil Funny", 5, language="en", rng=random.Random(2))
    assert all("Brazil Funny" in name for name in with_topic)


def test_sanitize_drops_official_and_repairs_candidates() -> None:
    from src.services.page_creation.naming import (
        is_weak_topic,
        name_repair_candidates,
        sanitize_page_name,
        usable_topic,
    )
    from src.services.page_creation.page_details import fill_missing_details, pick_page_category

    assert sanitize_page_name("Solace News Official") == "Solace News"
    assert sanitize_page_name("Hài Clip Chính Thức") == "Hài Clip"
    assert sanitize_page_name("Tài khoản mới Quê Tin Mới") == "Quê Tin Mới"
    assert "Official" not in " ".join(generate_page_names("News", 40, language="en", rng=random.Random(3)))
    candidates = name_repair_candidates("Solace News Official")
    assert candidates[0] == "Solace News"
    assert "Solace News Official" in candidates
    assert is_weak_topic("Tài khoản mới")
    assert usable_topic("Tài khoản mới") == ""
    assert usable_topic("Brazil Funny") == "Brazil Funny"
    weak_names = generate_page_names("Tài khoản mới", 8, language="vi", rng=random.Random(11))
    assert len(weak_names) == 8
    assert all("Tài khoản" not in name for name in weak_names)
    assert pick_page_category("Solace News", "en")
    assert pick_page_category("Hài Clip", "vi")
    details = fill_missing_details("Solace News", {"bio": ""}, language="en", rng=random.Random(5))
    assert details["bio"] and details["phone"] and details["email"]


def test_vietnamese_topic_uses_readable_suffix() -> None:
    names = generate_page_names("Hài Brazil", 3, rng=random.Random(2))
    assert all("Hài" in name and "Brazil" in name for name in names)
    joined = " ".join(names)
    assert any(suffix in joined for suffix in suffixes_for_topic("Hài Brazil"))


def test_account_label_leads_with_name() -> None:
    label = account_display_label(
        {"id": "acc_01", "name": "Brazil Ads", "email": "ads@example.com", "facebook_uid": "10001"}
    )
    assert label.startswith("Brazil Ads")
    assert "ads@example.com" in label
    assert "10001" in label
    assert topic_from_account_name("ads@example.com") == "ads"
    assert topic_from_account_name("10009999") == ""
    assert topic_from_account_name("acc_04e7df5e18") == ""
    plain = generate_page_names("acc_04e7df5e18", 12, language="mix", rng=random.Random(4))
    assert len(plain) == 12
    assert all("04e7" not in name.casefold() and "acc_" not in name.casefold() for name in plain)
    assert all("acc " not in f"{name.casefold()} " for name in plain)
    assert business_display_label("BM Brazil", "123456789") == "BM Brazil · 123456789"


def test_thousands_of_vietnamese_and_english_names() -> None:
    import time

    assert name_capacity("Brazil Funny", "en") >= 3000
    assert name_capacity("Hài", "vi") >= 3000
    assert name_capacity("", "mix") >= 5000
    started = time.perf_counter()
    english = generate_page_names("Brazil Funny", 1500, language="en", rng=random.Random(4))
    vietnamese = generate_page_names("Hài", 1500, language="vi", rng=random.Random(4))
    mixed = generate_page_names("Clip", 40, language="mix", rng=random.Random(4))
    elapsed = time.perf_counter() - started
    assert len(english) == 1500
    assert len({name.casefold() for name in english}) == 1500
    assert all("Brazil Funny" in name for name in english)
    assert len(vietnamese) == 1500
    assert all("Hài" in name for name in vietnamese)
    assert any(suffix in " ".join(vietnamese) for suffix in suffixes_for_topic("Hài"))
    assert elapsed < 2
    assert len(mixed) == 40
    assert any(any("à" <= ch <= "ỹ" or ch in "ăâêôơưđ" for ch in name) for name in mixed)
    assert any(name.isascii() for name in mixed)


def test_business_manager_store_keeps_one_row_per_id(tmp_path: Path) -> None:
    from src.services.page_creation.business_creator import extract_business_id
    from src.services.page_creation.business_store import BusinessManagerStore

    store = BusinessManagerStore(tmp_path / "business_managers.json")
    store.save(account_id="acc", business_id="123456789", business_name="BM Brazil")
    store.save(account_id="acc", business_id="123456789", business_name="BM Brazil Mới")
    store.save(account_id="other", business_id="123456789", business_name="BM Khác")
    rows = store.for_account("acc")
    assert len(rows) == 1
    assert rows[0]["business_name"] == "BM Brazil Mới"
    assert extract_business_id("https://business.facebook.com/latest/settings?business_id=99887766") == "99887766"
    assert extract_business_id("https://business.facebook.com/overview") == ""


def test_page_creation_ui_prefs_persist_avatar_dir(tmp_path: Path) -> None:
    from src.services.page_creation.store import PageCreationStore

    store = PageCreationStore(tmp_path / "page_creation.json")
    store.save_ui_prefs({"avatar_dir": r"C:\Users\Hello\Desktop\cartoonset10k", "name_lang": "en"})
    prefs = store.ui_prefs()
    assert prefs["avatar_dir"] == r"C:\Users\Hello\Desktop\cartoonset10k"
    assert prefs["name_lang"] == "en"
    store.save_ui_prefs({"topic": "Travel"})
    again = PageCreationStore(tmp_path / "page_creation.json").ui_prefs()
    assert again["avatar_dir"] == r"C:\Users\Hello\Desktop\cartoonset10k"
    assert again["topic"] == "Travel"
    store.save_ui_prefs(
        {
            "delay_enabled": True,
            "delay_mode": "range",
            "delay_min_seconds": "15",
            "delay_max_seconds": "40",
            "continue_on_error": True,
            "max_retry": "1",
            "admin_targets_text": "",
        }
    )
    kept = PageCreationStore(tmp_path / "page_creation.json").ui_prefs()
    assert kept["delay_mode"] == "range"
    assert kept["delay_min_seconds"] == "15"
    assert kept["delay_max_seconds"] == "40"
    assert kept["max_retry"] == "1"
    assert kept["name_lang"] == "en"


def test_create_business_ignores_invite_friends_dialog() -> None:
    from src.services.page_creation.business_creator import (
        is_business_name_field,
        is_create_portfolio_label,
        is_dismiss_label,
        is_invite_friends_dialog,
        is_wizard_advance_label,
        new_business_created,
    )

    dialog = "Mời bạn bè trên Facebook\nHãy mời bạn bè theo dõi Văn Thuy Trần"
    assert is_invite_friends_dialog(dialog)
    assert not is_create_portfolio_label("Bắt đầu trên Meta Business Suite")
    assert not is_create_portfolio_label("Mời bạn bè")
    assert not is_wizard_advance_label("Mời bạn bè")
    assert is_dismiss_label("Hủy")
    assert is_create_portfolio_label("Tạo danh mục doanh nghiệp")
    assert is_create_portfolio_label("Create a business portfolio")
    assert is_wizard_advance_label("Tiếp tục")
    assert not is_business_name_field("Tìm kiếm bạn bè")
    assert is_business_name_field("Tên danh mục doanh nghiệp")
    assert new_business_created("111", "111") is False
    assert new_business_created("111", "222") is True
    assert new_business_created("", "222") is True
    from src.services.page_creation.business_creator import business_preview_url

    assert business_preview_url("") == "https://business.facebook.com/latest/home"
    assert business_preview_url("99887766") == "https://business.facebook.com/latest/home?business_id=99887766"
    from src.services.page_creation.business_creator import portfolios_from_markup

    markup = """
    <div data-surface="lib:business_scope:page:555555555:Mot Page"></div>
    <div data-surface="lib:business_scope:portfolio:123456789:Van%20Thuy%20Tran"></div>
    <a href="https://business.facebook.com/latest/home?business_id=123456789"></a>
    ID danh mục doanh nghiệp: 123456789
    """
    found = portfolios_from_markup(markup)
    assert found == [{"business_id": "123456789", "business_name": "Van Thuy Tran"}]
    labeled = portfolios_from_markup("ID doanh nghiệp\n9988776655")
    assert labeled == [{"business_id": "9988776655", "business_name": ""}]


def test_random_avatar_comes_from_folder(tmp_path: Path) -> None:
    _png(tmp_path / "a.png")
    _png(tmp_path / "b.png")
    (tmp_path / "note.txt").write_text("x", encoding="utf-8")
    images = image_files_in_folder(str(tmp_path))
    assert len(images) == 2
    chosen = assign_random_avatars(5, images, rng=random.Random(3))
    assert len(chosen) == 5
    assert set(chosen) <= set(images)
    assert set(chosen) == set(images)
