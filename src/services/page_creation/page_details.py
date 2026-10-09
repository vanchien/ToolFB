"""Sinh thông tin Page (mô tả, SĐT, email, website, địa chỉ) theo tiếng Việt hoặc tiếng Anh."""

from __future__ import annotations

import random
import re
import unicodedata

from .naming import _has_vietnamese, resolve_name_language

_VI_BIO = (
    "{name} mang đến nội dung gần gũi mỗi ngày cho cộng đồng yêu thích.",
    "Theo dõi {name} để cập nhật khoảnh khắc mới, mẹo hay và câu chuyện thú vị.",
    "{name} chia sẻ cảm hứng, thông tin hữu ích và góc nhìn đời thường.",
    "Trang {name} dành cho người muốn xem nhanh những điều đáng chú ý hôm nay.",
    "{name} kết nối cộng đồng qua nội dung dễ xem, dễ nhớ và dễ chia sẻ.",
)
_EN_BIO = (
    "{name} brings fresh daily content for people who love quick, useful stories.",
    "Follow {name} for highlights, tips, and moments worth sharing.",
    "{name} shares inspiration, practical updates, and everyday ideas.",
    "{name} is a page for viewers who want clear content without the noise.",
    "Stay with {name} for friendly updates made for real people.",
)

_VI_STREETS = (
    "Nguyễn Huệ", "Lê Lợi", "Trần Hưng Đạo", "Hai Bà Trưng", "Điện Biên Phủ",
    "Cách Mạng Tháng Tám", "Nguyễn Thị Minh Khai", "Pasteur", "Võ Văn Tần", "Lý Thường Kiệt",
)
_VI_WARDS = (
    "Phường Bến Nghé", "Phường Đa Kao", "Phường 1", "Phường 3", "Phường Tân Định",
)
_VI_CITIES = (
    "Quận 1, TP. Hồ Chí Minh", "Quận 3, TP. Hồ Chí Minh", "Hà Nội", "Đà Nẵng", "Cần Thơ",
)

_EN_STREETS = (
    "Main Street", "Market Avenue", "Oak Lane", "Harbor Road", "Cedar Court",
    "Maple Drive", "Sunset Boulevard", "River Walk", "Park Place", "Hillcrest Way",
)
_EN_CITIES = (
    "Austin, TX", "Seattle, WA", "Denver, CO", "Portland, OR", "Chicago, IL",
    "Boston, MA", "Miami, FL", "San Diego, CA", "Nashville, TN", "Atlanta, GA",
)

_EMAIL_DOMAINS_EN = ("example.com", "mail.com", "pagehub.net", "studio.io", "media.co")
_EMAIL_DOMAINS_VI = ("email.vn", "mail.vn", "trang.vn", "kenh.net", "media.vn")

# Hạng mục Page phổ biến — ưu tiên mục Facebook thường hiện trong dropdown.
# Tránh mục chỉ gõ được chữ mà không có option (dễ kẹt kiểu «Interest» chưa chọn).
_PAGE_CATEGORIES = (
    ("Product/service", "Sản phẩm/Dịch vụ"),
    ("Community", "Cộng đồng"),
    ("Brand", "Thương hiệu"),
    ("Entertainment website", "Trang web giải trí"),
    ("Media/news company", "Công ty truyền thông/tin tức"),
    ("Digital creator", "Người sáng tạo nội dung số"),
    ("Arts & entertainment", "Nghệ thuật & giải trí"),
    ("Blog", "Blog"),
    ("Education website", "Trang web giáo dục"),
)

# Dự phòng khi mục đã chọn không có trong list Facebook.
_CATEGORY_FALLBACKS_EN = (
    "Product/service",
    "Community",
    "Brand",
    "Entertainment website",
    "Blog",
)
_CATEGORY_FALLBACKS_VI = (
    "Sản phẩm/Dịch vụ",
    "Cộng đồng",
    "Thương hiệu",
    "Trang web giải trí",
    "Blog",
)


def _strip_accents(text: str) -> str:
    normalized = unicodedata.normalize("NFD", text or "")
    return "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn").replace("đ", "d").replace("Đ", "D")


def slug_from_page_name(page_name: str) -> str:
    """Đưa tên Page về chuỗi dùng cho email/website."""
    raw = _strip_accents(page_name or "").casefold()
    raw = re.sub(r"[^a-z0-9]+", ".", raw).strip(".")
    parts = [part for part in raw.split(".") if part]
    if not parts:
        return "page"
    return ".".join(parts[:4])[:40].strip(".")


def generate_phone(language: str, *, rng: random.Random | None = None) -> str:
    """Sinh số điện thoại giả lập theo vùng VN hoặc US."""
    picker = rng or random.Random()
    if language == "vi":
        head = picker.choice(("90", "91", "93", "94", "96", "97", "98", "70", "79", "77", "76", "78", "32", "33", "34", "35", "36", "37", "38", "39"))
        mid = picker.randint(100, 999)
        tail = picker.randint(1000, 9999)
        return f"+84 {head} {mid} {tail}"
    area = picker.choice(("201", "212", "213", "305", "312", "415", "512", "617", "702", "718", "818", "916"))
    mid = picker.randint(200, 998)
    if mid == 555:
        mid = 556
    tail = picker.randint(1000, 9999)
    return f"+1 ({area}) {mid}-{tail}"


def generate_email(page_name: str, language: str, *, rng: random.Random | None = None) -> str:
    """Email theo tên Page, miền .vn hoặc .com."""
    picker = rng or random.Random()
    slug = slug_from_page_name(page_name) or "page"
    local = slug.replace(".", "")[:18] or "page"
    suffix = picker.randint(10, 99)
    domain = picker.choice(_EMAIL_DOMAINS_VI if language == "vi" else _EMAIL_DOMAINS_EN)
    return f"contact.{local}{suffix}@{domain}"


def generate_website(page_name: str, language: str, *, rng: random.Random | None = None) -> str:
    """Website giả lập gắn với tên Page."""
    picker = rng or random.Random()
    slug = slug_from_page_name(page_name).replace(".", "")[:24] or "page"
    tld = picker.choice((".vn", ".com.vn", ".net")) if language == "vi" else picker.choice((".com", ".net", ".co"))
    return f"https://www.{slug}{tld}"


def generate_address(language: str, *, rng: random.Random | None = None) -> str:
    """Địa chỉ giả lập theo ngôn ngữ."""
    picker = rng or random.Random()
    number = picker.randint(12, 298)
    if language == "vi":
        street = picker.choice(_VI_STREETS)
        ward = picker.choice(_VI_WARDS)
        city = picker.choice(_VI_CITIES)
        return f"{number} {street}, {ward}, {city}"
    street = picker.choice(_EN_STREETS)
    city = picker.choice(_EN_CITIES)
    return f"{number} {street}, {city}"


def generate_bio(page_name: str, language: str, *, rng: random.Random | None = None) -> str:
    """Mô tả ngắn cho ô Bio / Tiểu sử (≤255 ký tự — giới hạn form tạo Page)."""
    picker = rng or random.Random()
    template = picker.choice(_VI_BIO if language == "vi" else _EN_BIO)
    text = template.format(name=page_name.strip() or ("Trang" if language == "vi" else "Page"))
    if len(text) > 255:
        text = text[:252].rstrip() + "..."
    return text


def category_label_for_form(preferred: str, *, english: bool) -> str:
    """
    Đổi nhãn hạng mục sang đúng ngôn ngữ form Facebook.

    Form EN mà gõ «Trang web giải trí» sẽ ra dropdown Local service / Shopping.
    """
    text = " ".join((preferred or "").split())
    if not text:
        return "Product/service" if english else "Sản phẩm/Dịch vụ"
    target = text.casefold().replace("／", "/")
    for en_label, vi_label in _PAGE_CATEGORIES:
        en_n = en_label.casefold().replace("／", "/")
        vi_n = vi_label.casefold().replace("／", "/")
        if target in {en_n, vi_n} or target == en_n or target == vi_n:
            return en_label if english else vi_label
    return text


def category_fallbacks(language: str) -> tuple[str, ...]:
    """Danh sách hạng mục dự phòng khi mục đầu không chọn được trên form."""
    return _CATEGORY_FALLBACKS_VI if language == "vi" else _CATEGORY_FALLBACKS_EN


def pick_page_category(page_name: str, language: str, *, rng: random.Random | None = None) -> str:
    """Chọn hạng mục theo tên Page (ổn định) hoặc ngẫu nhiên khi có rng."""
    if not _PAGE_CATEGORIES:
        return "Product/service"
    if rng is not None:
        en_label, vi_label = rng.choice(_PAGE_CATEGORIES)
    else:
        digest = 0
        for ch in (page_name or "page").casefold():
            digest = (digest * 33 + ord(ch)) & 0xFFFFFFFF
        en_label, vi_label = _PAGE_CATEGORIES[digest % len(_PAGE_CATEGORIES)]
    return vi_label if language == "vi" else en_label


def categories_are_same(left: str, right: str) -> bool:
    """Cùng một hạng mục, kể cả cặp EN/VI. Không khớp chuỗi con («Website» ≠ «Entertainment website»)."""

    def _norm(value: str) -> str:
        return " ".join((value or "").casefold().replace("／", "/").split())

    a = _norm(left)
    b = _norm(right)
    if not a or not b:
        return False
    if a == b:
        return True
    for en_label, vi_label in _PAGE_CATEGORIES:
        pair = {_norm(en_label), _norm(vi_label)}
        if a in pair and b in pair:
            return True
    return False


def category_pool_labels() -> tuple[str, ...]:
    """Mọi nhãn hạng mục (EN + VI) dùng để nhận chip đã chọn trên form."""
    labels: list[str] = []
    for en_label, vi_label in _PAGE_CATEGORIES:
        labels.append(en_label)
        if vi_label != en_label:
            labels.append(vi_label)
    return tuple(labels)


def details_language_for_name(page_name: str, language: str | None) -> str:
    """Chọn vi/en cho từng Page khi chế độ mix."""
    picked = (language or "").strip().lower()
    if picked in {"vi", "en"}:
        return picked
    from .naming import _has_vietnamese

    if picked == "mix":
        return "vi" if _has_vietnamese(page_name) else "en"
    return resolve_name_language(language, page_name)


def generate_page_details(
    page_name: str,
    *,
    language: str | None = None,
    rng: random.Random | None = None,
) -> dict[str, str]:
    """Một bộ thông tin đầy đủ cho một Page."""
    picker = rng or random.Random()
    lang = details_language_for_name(page_name, language)
    return {
        "category": pick_page_category(page_name, lang, rng=picker),
        "bio": generate_bio(page_name, lang, rng=picker),
        "phone": generate_phone(lang, rng=picker),
        "email": generate_email(page_name, lang, rng=picker),
        "website": generate_website(page_name, lang, rng=picker),
        "address": generate_address(lang, rng=picker),
    }


_DETAIL_KEYS = ("category", "bio", "phone", "email", "website", "address")


def fill_missing_details(
    page_name: str,
    current: dict[str, str] | None = None,
    *,
    language: str | None = None,
    rng: random.Random | None = None,
) -> dict[str, str]:
    """
    Bổ sung trường trống (bio/phone/email/…) từ bộ sinh.

    Giữ nguyên giá trị đã có. Không tên Page thì trả bản sao ``current``.
    """
    base = {key: str((current or {}).get(key) or "").strip() for key in _DETAIL_KEYS}
    name = (page_name or "").strip()
    if not name:
        return base
    if all(base.values()):
        return base
    generated = generate_page_details(name, language=language, rng=rng)
    for key in _DETAIL_KEYS:
        if not base[key]:
            base[key] = str(generated.get(key) or "").strip()
    return base


def attach_generated_details(
    rows: list[dict[str, str]],
    *,
    language: str | None = None,
    enabled: bool = True,
    rng: random.Random | None = None,
) -> list[dict[str, str]]:
    """Gắn thông tin tự sinh vào từng dòng Page. Tắt thì để trống."""
    picker = rng or random.Random()
    out: list[dict[str, str]] = []
    for row in rows:
        item = dict(row)
        name = str(item.get("page_name") or "").strip()
        if enabled and name:
            item.update(generate_page_details(name, language=language, rng=picker))
        else:
            for key in ("category", "bio", "phone", "email", "website", "address"):
                item.setdefault(key, "")
        out.append(item)
    return out


def summarize_page_details(row: dict[str, str]) -> str:
    """Chuỗi ngắn hiện trên bảng danh sách."""
    parts = [
        str(row.get("category") or "").strip(),
        str(row.get("phone") or "").strip(),
        str(row.get("email") or "").strip(),
    ]
    filled = [part for part in parts if part]
    if not filled:
        bio = str(row.get("bio") or "").strip()
        return (bio[:40] + "…") if len(bio) > 40 else bio
    return " · ".join(filled[:2])
