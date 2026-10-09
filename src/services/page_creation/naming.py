"""Sinh tên Page và gán ảnh ngẫu nhiên từ một thư mục."""

from __future__ import annotations

import random
import re
from pathlib import Path

from .validate import IMAGE_EXT, avatar_is_image

_EN_LEADS = (
    "Funny", "Happy", "Wild", "Cute", "Epic", "Super", "Lucky", "Golden", "Bright", "Cosmic",
    "Urban", "Classic", "Modern", "Secret", "Mighty", "Gentle", "Brave", "Royal", "Sunny", "Neon",
    "Retro", "Fresh", "Prime", "Nova", "Metro", "Lunar", "Crystal", "Jolly", "Rapid", "Silver",
    "Pacific", "Nordic", "Safari", "Velvet", "Amber", "Coral", "Maple", "River", "Summit", "Harbor",
    "Echo", "Orbit", "Pixel", "Vista", "Cedar", "Maplewood", "Kinetic", "Aurora", "Nimbus", "Solace",
    "Bold", "Chill", "Daring", "Fancy", "Grand", "Hidden", "Iconic", "Jazzy", "Kindred", "Lively",
)
_EN_CORES = (
    "Clips", "Movies", "Moments", "Stories", "Shorts", "Laughs", "World", "Zone", "Hub", "Club",
    "Channel", "Studio", "Fans", "Feed", "Room", "Night", "Insider", "Network", "Highlights", "Animals",
    "Comedy", "Drama", "Travel", "Food", "Music", "Sport", "News", "Family", "Pets", "Nature",
    "City", "Cinema", "Reels", "Scenes", "Voices", "Frames", "Tales", "Beats", "Trails", "Kitchen",
)
_EN_TAILS = (
    "Daily", "Today", "Now", "Plus", "Online", "Extra", "Select", "Max", "Pro",
    "Weekly", "Nightly", "Live", "TV", "Media", "Hub", "World", "Zone", "Club", "Studio",
    "Hour", "Post", "Time", "Spot", "Desk", "Wire", "Cast", "Show", "Reel", "Cut",
)
_VI_LEADS = (
    "Hài", "Vui", "Dễ Thương", "Hoang Dã", "Siêu", "May Mắn", "Vàng", "Sáng", "Cổ Điển", "Hiện Đại",
    "Bí Mật", "Mạnh Mẽ", "Dịu Dàng", "Dũng Cảm", "Hoàng Gia", "Nắng", "Tươi", "Mới", "Đô Thị", "Biển",
    "Núi", "Sông", "Phố", "Đêm", "Bình Minh", "Hoài Niệm", "Nhanh", "Ấm", "Lành", "Rộn Ràng",
    "Thú Vị", "Độc Đáo", "Gần Gũi", "Bất Ngờ", "Nhẹ Nhàng", "Sôi Động", "Yên", "Xanh", "Ngọt", "Lạ",
    "Đáng Yêu", "Hồn Nhiên", "Chất", "Đỉnh", "Chill", "Ngầu", "Mộc", "Trong Trẻo", "Rực Rỡ", "Bình Yên",
)
_VI_CORES = (
    "Clip", "Phim", "Khoảnh Khắc", "Câu Chuyện", "Tiếng Cười", "Thế Giới", "Góc", "Cộng Đồng", "Kênh",
    "Xưởng", "Người Hâm Mộ", "Bảng Tin", "Đêm", "Động Vật", "Hài Kịch", "Du Lịch", "Ẩm Thực", "Nhạc",
    "Thể Thao", "Tin", "Gia Đình", "Thú Cưng", "Thiên Nhiên", "Thành Phố", "Điện Ảnh", "Khung Hình",
    "Giọng Nói", "Nhịp", "Hành Trình", "Bếp", "Sân Khấu", "Phố Xá", "Quê", "Biển Cả", "Rừng", "Vườn",
    "Bạn Bè", "Tuổi Thơ", "Cuối Tuần", "Đời Thường",
)
_VI_TAILS = (
    "Hôm Nay", "Mỗi Ngày", "Clip Hay", "Tin Mới", "Nổi Bật", "Hàng Ngày", "Mới Nhất",
    "Cùng Xem", "Đáng Xem", "Cuối Tuần", "Buổi Tối", "Sưu Tầm", "Trực Tiếp", "Plus",
    "TV", "Media", "Studio", "Moments", "Giờ Vàng", "Bản Đẹp", "Tuyển Chọn", "Đặc Sắc",
    "Mới Lên", "Xem Ngay", "Hay Nhất", "Góc Nhỏ", "Nhà Mình",
)

# Từ / cụm Facebook hay báo tên không hợp lệ (vd. «… Official» → gợi ý bỏ Official).
_RESTRICTED_NAME_TOKENS = frozenset(
    {
        "official",
        "chinhthuc",
        "fanpage",
        "facebook",
        "meta",
        "instagram",
        "tiktok",
        "youtube",
    }
)
_RESTRICTED_NAME_PHRASES = (
    ("fan", "page"),
    ("official", "page"),
    ("chinh", "thuc"),
    ("trang", "chinh", "thuc"),
)


def _has_vietnamese(text: str) -> bool:
    for ch in text:
        if ch in "ăâêôơưđĂÂÊÔƠƯĐ":
            return True
        if "\u00c0" <= ch <= "\u1ef9":
            return True
    return False


def _token_key(word: str) -> str:
    """Chuẩn hóa token để so với danh sách từ bị hạn chế."""
    raw = "".join(ch for ch in (word or "").casefold() if ch.isalnum() or ch.isspace())
    if _has_vietnamese(word or ""):
        # bỏ dấu để khớp «Chính Thức» ↔ chinhthuc
        import unicodedata

        normalized = unicodedata.normalize("NFD", raw)
        raw = "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")
        raw = raw.replace("đ", "d")
    return "".join(raw.split())


def sanitize_page_name(name: str) -> str:
    """Bỏ token/cụm Facebook hay từ chối và cụm tên account mặc định khỏi tên Page."""
    words = [part for part in (name or "").split() if part]
    if not words:
        return ""
    keys = [_token_key(word) for word in words]
    drop = [False] * len(words)
    for phrase in (*_RESTRICTED_NAME_PHRASES, *_WEAK_TOPIC_PHRASES):
        span = len(phrase)
        for start in range(0, len(keys) - span + 1):
            if tuple(keys[start : start + span]) == phrase:
                for index in range(start, start + span):
                    drop[index] = True
    kept = [
        word
        for word, key, skip in zip(words, keys, drop)
        if not skip and key not in _RESTRICTED_NAME_TOKENS
    ]
    cleaned = " ".join(kept).strip()
    if cleaned and not is_weak_topic(cleaned):
        return cleaned
    fallback = [word for word, skip in zip(words, drop) if not skip]
    alt = " ".join(fallback).strip()
    if alt and not is_weak_topic(alt):
        return alt
    return ""


def name_repair_candidates(name: str) -> list[str]:
    """
    Các tên thay thế khi Facebook báo tên không hợp lệ.

    Thứ tự: tên đã làm sạch → bỏ từ cuối → bỏ 2 từ cuối.
    """
    original = " ".join((name or "").split())
    if not original:
        return []
    cleaned = sanitize_page_name(original)
    out: list[str] = []
    for candidate in (cleaned, original):
        if candidate and candidate not in out:
            out.append(candidate)
    words = cleaned.split()
    if len(words) >= 3:
        shorter = " ".join(words[:-1])
        if shorter and shorter not in out:
            out.append(shorter)
    if len(words) >= 4:
        shorter = " ".join(words[:-2])
        if shorter and shorter not in out:
            out.append(shorter)
    return out


def suffixes_for_topic(topic: str) -> tuple[str, ...]:
    """Hậu tố tên Page theo ngôn ngữ của chủ đề."""
    if _has_vietnamese(topic):
        return _VI_TAILS
    return _EN_TAILS


def _pools(language: str) -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    if language == "vi":
        return _VI_LEADS, _VI_CORES, _VI_TAILS
    return _EN_LEADS, _EN_CORES, _EN_TAILS


def resolve_name_language(language: str | None, topic: str) -> str:
    """``vi``, ``en`` hoặc ``mix``. Để trống thì theo chữ trong chủ đề."""
    picked = (language or "").strip().lower()
    if picked in {"vi", "en", "mix"}:
        return picked
    if _has_vietnamese(topic):
        return "vi"
    return "en"


def topic_for_language(topic: str, language: str) -> str:
    """
    Chủ đề chỉ gắn vào tên khi khớp ngôn ngữ đã chọn.

    Chọn tiếng Anh mà chủ đề có dấu Việt (vd. tên tài khoản) thì bỏ chủ đề,
    tránh tên kiểu ``Iconic Tài khoản mới Today``.
    """
    cleaned = usable_topic(topic)
    if not cleaned:
        return ""
    if language == "en" and _has_vietnamese(cleaned):
        return ""
    return cleaned


def _join_name(*parts: str) -> str:
    words: list[str] = []
    for part in parts:
        for word in part.split():
            if words and words[-1].casefold() == word.casefold():
                continue
            words.append(word)
    return " ".join(words)


def _pattern_sizes(language: str, topic: str) -> list[tuple[str, int]]:
    leads, cores, tails = _pools(language)
    sizes: list[tuple[str, int]] = []
    if topic:
        sizes.append(("lead_topic_tail", len(leads) * len(tails)))
        sizes.append(("topic_core_tail", len(cores) * len(tails)))
    sizes.append(("lead_core", len(leads) * len(cores)))
    sizes.append(("lead_core_tail", len(leads) * len(cores) * len(tails)))
    return sizes


def name_capacity(topic: str = "", language: str | None = None) -> int:
    """Số tên khác nhau có thể sinh, không cần dựng hết danh sách."""
    cleaned = usable_topic(topic)
    lang = resolve_name_language(language, cleaned)
    langs = ("vi", "en") if lang == "mix" else (lang,)
    return sum(
        size
        for code in langs
        for _name, size in _pattern_sizes(code, topic_for_language(cleaned, code))
    )


def _name_at(language: str, topic: str, index: int) -> str:
    leads, cores, tails = _pools(language)
    n_leads = len(leads)
    n_cores = len(cores)
    n_tails = len(tails)
    cursor = index
    if topic:
        topic_span = n_leads * n_tails
        if cursor < topic_span:
            lead_i, tail_i = divmod(cursor, n_tails)
            return _join_name(leads[lead_i], topic, tails[tail_i])
        cursor -= topic_span
        core_span = n_cores * n_tails
        if cursor < core_span:
            core_i, tail_i = divmod(cursor, n_tails)
            return _join_name(topic, cores[core_i], tails[tail_i])
        cursor -= core_span
    lead_core = n_leads * n_cores
    if cursor < lead_core:
        lead_i, core_i = divmod(cursor, n_cores)
        return _join_name(leads[lead_i], cores[core_i])
    cursor -= lead_core
    lead_i, rem = divmod(cursor, n_cores * n_tails)
    core_i, tail_i = divmod(rem, n_tails)
    if lead_i >= n_leads:
        return ""
    return _join_name(leads[lead_i], cores[core_i], tails[tail_i])


def generate_page_names(
    topic: str,
    count: int,
    *,
    language: str | None = None,
    rng: random.Random | None = None,
    avoid: list[str] | None = None,
) -> list[str]:
    """
    Sinh tới ``count`` tên Page khác nhau.

    Có chủ đề thì tên chứa chủ đề. Không chủ đề thì lấy từ kho tiếng Việt hoặc tiếng Anh.
    Kho tổ hợp nằm trong RAM theo chỉ số, không tạo sẵn hàng chục nghìn chuỗi.
    """
    cleaned = usable_topic(topic)
    if count < 1:
        return []
    lang = resolve_name_language(language, cleaned)
    langs = ("vi", "en") if lang == "mix" else (lang,)
    picker = rng or random.Random()
    seen = {item.strip().casefold() for item in (avoid or []) if item and item.strip()}
    if lang == "mix":
        first = (count + 1) // 2
        vi_names = _take_names("vi", topic_for_language(cleaned, "vi"), first, picker, seen)
        en_names = _take_names(
            "en",
            topic_for_language(cleaned, "en"),
            count - len(vi_names),
            picker,
            seen,
        )
        mixed: list[str] = []
        for index in range(max(len(vi_names), len(en_names))):
            if index < len(vi_names):
                mixed.append(vi_names[index])
            if index < len(en_names) and len(mixed) < count:
                mixed.append(en_names[index])
        return mixed[:count]
    use_topic = topic_for_language(cleaned, langs[0])
    return _take_names(langs[0], use_topic, count, picker, seen)


def _take_names(
    language: str,
    topic: str,
    count: int,
    picker: random.Random,
    seen: set[str],
) -> list[str]:
    """Ưu tiên tổ hợp có chủ đề, hết thì lấy kho tên chung."""
    if count < 1:
        return []
    sizes = _pattern_sizes(language, topic)
    topic_n = 0
    if topic:
        topic_n = sizes[0][1] + sizes[1][1]
    total = sum(size for _name, size in sizes)
    order: list[int] = []
    if topic_n:
        topic_ids = list(range(topic_n))
        picker.shuffle(topic_ids)
        order.extend(topic_ids)
    if total > topic_n:
        catalog_ids = list(range(topic_n, total))
        if count > topic_n:
            picker.shuffle(catalog_ids)
            order.extend(catalog_ids)
    out: list[str] = []
    for index in order:
        name = _name_at(language, topic, index)
        key = name.casefold()
        if not name or key in seen or len(name) > 75:
            continue
        name = sanitize_page_name(name)
        key = name.casefold()
        if not name or key in seen or len(name) > 75:
            continue
        seen.add(key)
        out.append(name)
        if len(out) >= count:
            break
    return out


_ID_TOKEN = re.compile(r"^(?:acc[_-]?)?[0-9a-f]{6,}$|^\d{4,}$", re.I)

# Chủ đề lấy từ tên account mặc định — không dùng để ghép tên Page.
_WEAK_TOPICS = frozenset(
    {
        "tai khoan moi",
        "tài khoản mới",
        "new account",
        "account",
        "tai khoan",
        "tài khoản",
        "user",
        "test",
        "demo",
        "untitled",
        "facebook",
        "page",
        "trang",
        "acc",
    }
)
_WEAK_TOPIC_PHRASES = (
    ("tai", "khoan", "moi"),
    ("new", "account"),
)


def is_weak_topic(topic: str) -> bool:
    """True với tên account mặc định kiểu «Tài khoản mới» — không ghép vào tên Page."""
    text = " ".join((topic or "").split())
    if not text or len(text) < 3:
        return True
    spaced = " ".join(_token_key(word) for word in text.split())
    if spaced in _WEAK_TOPICS or text.casefold() in _WEAK_TOPICS:
        return True
    compact = _token_key(text.replace(" ", ""))
    if compact in {_token_key(item.replace(" ", "")) for item in _WEAK_TOPICS}:
        return True
    keys = [_token_key(word) for word in text.split()]
    for phrase in _WEAK_TOPIC_PHRASES:
        if len(keys) >= len(phrase) and tuple(keys[: len(phrase)]) == phrase and len(keys) == len(phrase):
            return True
    return False


def usable_topic(topic: str) -> str:
    """Chủ đề dùng để sinh tên. Bỏ topic yếu / tiền tố «Tài khoản mới» / UID."""
    if not (topic or "").strip():
        return ""
    cleaned = topic_from_account_name(topic)
    words = cleaned.split()
    keys = [_token_key(word) for word in words]
    for phrase in _WEAK_TOPIC_PHRASES:
        span = len(phrase)
        if len(keys) >= span and tuple(keys[:span]) == phrase:
            cleaned = " ".join(words[span:]).strip()
            break
    if is_weak_topic(cleaned):
        return ""
    return cleaned


def topic_from_account_name(name: str) -> str:
    """Rút chủ đề đọc được. Bỏ email domain, UID, mã nội bộ, và tên account mặc định."""
    raw = (name or "").replace("_", " ").replace(".", " ").replace("-", " ")
    if "@" in raw:
        raw = raw.split("@", 1)[0]
    kept: list[str] = []
    for token in raw.split():
        if token.casefold() == "acc" or _ID_TOKEN.match(token):
            continue
        letters = sum(ch.isalpha() for ch in token)
        if letters < 2:
            continue
        if any(ch.isdigit() for ch in token) and len(token) >= 8:
            continue
        kept.append(token)
    cleaned = " ".join(kept)
    return "" if is_weak_topic(cleaned) else cleaned


def account_display_label(row: dict) -> str:
    """Tên tài khoản đứng trước, rồi email và UID để nhận ra trên combobox."""
    name = str(row.get("name") or "").strip()
    email = str(row.get("email") or "").strip()
    uid = str(row.get("facebook_uid") or "").strip()
    parts: list[str] = []
    for part in (name, email, uid):
        if part and part not in parts:
            parts.append(part)
    if parts:
        return " · ".join(parts)
    return str(row.get("id") or "").strip() or "Tài khoản"


def business_display_label(name: str, business_id: str) -> str:
    """Tên Business Manager đứng trước, id nằm sau để vẫn chọn đúng."""
    title = (name or "").strip()
    bid = (business_id or "").strip()
    if title and title != bid:
        return f"{title} · {bid}" if bid else title
    return bid or title


def image_files_in_folder(folder: str) -> list[str]:
    """Ảnh hợp lệ nằm trực tiếp trong thư mục, sắp theo tên."""
    root = Path(folder)
    if not root.is_dir():
        return []
    found: list[str] = []
    try:
        entries = list(root.iterdir())
    except OSError:
        return []
    for path in entries:
        if not path.is_file() or path.suffix.lower() not in IMAGE_EXT:
            continue
        text = str(path)
        if avatar_is_image(text):
            found.append(text)
    found.sort(key=lambda item: Path(item).name.casefold())
    return found


def assign_random_avatars(count: int, images: list[str], rng: random.Random | None = None) -> list[str]:
    """
    Gán mỗi Page một ảnh ngẫu nhiên trong thư mục.

    Đủ ảnh thì không lặp cho đến khi hết vòng. Thiếu ảnh thì xáo lại và dùng tiếp.
    """
    if count < 1:
        return []
    pool_source = [item for item in images if item]
    if not pool_source:
        return []
    picker = rng or random.Random()
    bag = list(pool_source)
    picker.shuffle(bag)
    chosen: list[str] = []
    while len(chosen) < count:
        if not bag:
            bag = list(pool_source)
            picker.shuffle(bag)
        chosen.append(bag.pop())
    return chosen
