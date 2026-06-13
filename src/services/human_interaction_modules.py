"""
Các module tương tác giống người dùng thật trên Facebook.

Dùng ``HumanAction`` (chuột Bezier, cuộn tự nhiên, gõ có typo) thay cho click/scroll thô.
"""

from __future__ import annotations

import random
import time
from typing import Callable

from loguru import logger
from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError

from src.automation.facebook_actions import human_pause
from src.services.human_ai_comment import generate_facebook_comment
from src.services.human_interaction_profile import HumanInteractionProfile, resolve_profile
from src.utils.human_action import HumanAction

StatusCallback = Callable[[str, str], None]

_REELS_KEYWORDS = (
    "reels hài",
    "nấu ăn",
    "gái xinh",
    "du lịch việt nam",
    "thể thao",
    "review đồ ăn",
    "ca nhạc",
    "phim hay",
)

_SEARCH_INPUT_SELECTORS = (
    '[aria-label*="Search" i][role="combobox"]',
    '[aria-label*="Tìm kiếm" i][role="combobox"]',
    'input[type="search"]',
    '[placeholder*="Search" i]',
    '[placeholder*="Tìm kiếm" i]',
)

_SEARCH_RESULTS_SELECTORS = (
    '[role="listbox"] [role="option"]',
    '[role="listbox"] a',
    '[role="main"] [role="article"]',
    '[role="feed"]',
    '[role="tab"]',
    'a[href*="/search/"]',
)


def deep_delay_between_modules(*, min_sec: float, max_sec: float) -> None:
    """Nghỉ sâu giữa các module lớn."""
    lo = max(0.1, float(min_sec))
    hi = max(lo, float(max_sec))
    sec = random.uniform(lo, hi)
    logger.info("[Human] Deep delay {:.1f}s giữa các module", sec)
    time.sleep(sec)


def _module_micro_pause(cfg: HumanInteractionProfile, *, label: str = "") -> None:
    """Tạm dừng ngắn trước/sau thao tác trong một module."""
    human_pause(kind="action", label=label or "module")
    time.sleep(random.uniform(cfg.module_pause_min_sec, cfg.module_pause_max_sec))


def _page_load_pause(cfg: HumanInteractionProfile) -> None:
    """Chờ ngắn sau goto — theo preset, tránh sleep cố định 3–5s mỗi module."""
    time.sleep(random.uniform(cfg.page_load_pause_min_sec, cfg.page_load_pause_max_sec))


def _reels_clip_wait(page: Page, cfg: HumanInteractionProfile) -> None:
    """Xem một clip Reels — thời lượng theo preset."""
    lo = max(1500, int(cfg.reels_clip_min_ms))
    hi = max(lo, int(cfg.reels_clip_max_ms))
    page.wait_for_timeout(random.randint(lo, hi))


def _scroll_feed_top_to_bottom(
    ha: HumanAction,
    cfg: HumanInteractionProfile,
    *,
    short: bool = False,
    like_rate: float = 0.0,
    comment_rate: float = 0.0,
    on_like: Callable[[Page], None] | None = None,
    on_comment: Callable[[Page], None] | None = None,
) -> None:
    """Cuộn từ đầu trang xuống — số vòng theo preset (safe/normal/fast)."""
    if short:
        lo, hi = cfg.scroll_rounds_short_min, cfg.scroll_rounds_short_max
    else:
        lo, hi = cfg.scroll_rounds_min, cfg.scroll_rounds_max
    rounds = random.randint(max(4, lo), max(lo, hi))
    ha.natural_scroll_feed(
        rounds=rounds,
        like_rate=like_rate,
        comment_rate=comment_rate,
        on_like=on_like,
        on_comment=on_comment,
        downward_bias=0.97,
        scroll_from_top=True,
        dwell_scale=cfg.dwell_scale,
    )
    human_pause(kind="step", label="sau cuộn feed")


def _human(page: Page, cfg: HumanInteractionProfile) -> HumanAction:
    return HumanAction(page, show_cursor=cfg.virtual_cursor)


def _visible_article_text(page: Page) -> str:
    for sel in ('[role="article"]', '[data-pagelet*="FeedUnit"]'):
        loc = page.locator(sel).first
        try:
            if loc.is_visible(timeout=1200):
                return (loc.inner_text(timeout=2000) or "").strip()
        except Exception:
            continue
    return ""


def _try_like_visible_post(page: Page, ha: HumanAction) -> None:
    like = page.locator(
        '[aria-label*="Like" i]:not([aria-pressed="true"]), '
        '[aria-label*="Thích" i]:not([aria-pressed="true"]), '
        '[role="button"][aria-label*="Like" i], '
        '[role="button"][aria-label*="Thích" i]'
    ).first
    try:
        if like.is_visible(timeout=1500):
            ha.smart_click(like, label="like bài")
            human_pause(kind="click", label="like bài")
    except PlaywrightTimeoutError:
        pass


def _try_comment_visible_post(page: Page, ha: HumanAction, *, use_ai: bool) -> None:
    article = page.locator('[role="article"]').first
    post_text = _visible_article_text(page)
    if use_ai:
        comment = generate_facebook_comment(post_text)
    else:
        comment = ""
    if not comment:
        return
    try:
        comment_btn = article.locator(
            '[aria-label*="Comment" i], [aria-label*="Bình luận" i], '
            '[role="button"]:has-text("Comment"), [role="button"]:has-text("Bình luận")'
        ).first
        if comment_btn.is_visible(timeout=1500):
            ha.smart_click(comment_btn, label="mở comment")
            human_pause(kind="click", label="comment box")
    except PlaywrightTimeoutError:
        pass
    box = page.locator(
        '[role="textbox"][contenteditable="true"][aria-label*="Comment" i], '
        '[role="textbox"][contenteditable="true"][aria-label*="Bình luận" i], '
        '[role="textbox"][contenteditable="true"]'
    ).last
    try:
        if box.is_visible(timeout=2500):
            ha.smart_click(box, label="ô comment")
            from src.utils.human_typing import human_type_locator

            human_type_locator(box, comment, submit_enter=True, clear_first=True, label="AI comment")
            human_pause(kind="input", label="gửi comment")
            logger.info("[Human] Đã comment AI: {}", comment[:50])
    except PlaywrightTimeoutError:
        pass


def module_newsfeed_like(page: Page, *, probability: float = 0.70, cfg: HumanInteractionProfile | None = None) -> bool:
    """Lướt Newsfeed tự nhiên + Like/Comment theo tỷ lệ cấu hình."""
    if random.random() > probability:
        logger.info("[Human] Bỏ qua module Newsfeed/Like (xác suất)")
        return False
    profile = cfg or resolve_profile("normal")
    ha = _human(page, profile)
    logger.info(
        "[Human] Module Newsfeed — like {:.0f}% comment {:.0f}%",
        profile.like_rate_pct * 100,
        profile.comment_rate_pct * 100,
    )
    try:
        page.goto("https://www.facebook.com/", wait_until="domcontentloaded", timeout=60_000)
        _module_micro_pause(profile, label="newsfeed_load")
        _page_load_pause(profile)

        def on_like(p: Page) -> None:
            _try_like_visible_post(p, _human(p, profile))

        def on_comment(p: Page) -> None:
            if profile.ai_comments:
                _try_comment_visible_post(p, _human(p, profile), use_ai=True)

        _scroll_feed_top_to_bottom(
            ha,
            profile,
            like_rate=profile.like_rate_pct,
            comment_rate=profile.comment_rate_pct if profile.ai_comments else 0.0,
            on_like=on_like,
            on_comment=on_comment if profile.ai_comments else None,
        )
        human_pause(label="cuối newsfeed", kind="step")
        time.sleep(random.uniform(0.8, 1.6))
        return True
    except Exception as exc:  # noqa: BLE001
        logger.warning("[Human] Newsfeed/Like lỗi: {}", exc)
        return False


def _wait_search_results_ready(
    page: Page,
    cfg: HumanInteractionProfile | None = None,
    *,
    timeout_ms: int = 15_000,
) -> None:
    """Chờ trang / dropdown kết quả tìm kiếm ổn định trước click tab hoặc link."""
    profile = cfg or resolve_profile("normal")
    deadline = time.monotonic() + max(3.0, timeout_ms / 1000.0)
    settled = False
    while time.monotonic() < deadline:
        url = (page.url or "").lower()
        if "/search" in url or "q=" in url:
            settled = True
            break
        for sel in _SEARCH_RESULTS_SELECTORS:
            try:
                if page.locator(sel).first.is_visible(timeout=900):
                    settled = True
                    break
            except Exception:
                continue
        if settled:
            break
        page.wait_for_timeout(450)
    human_pause(kind="step", label="kết quả tìm kiếm")
    _page_load_pause(profile)


def _open_search_and_type(
    page: Page,
    ha: HumanAction,
    keyword: str,
    cfg: HumanInteractionProfile,
) -> bool:
    """Click ô tìm kiếm, gõ **đủ** từ khóa (chậm), verify, Enter, chờ kết quả."""
    search = None
    for sel in _SEARCH_INPUT_SELECTORS:
        loc = page.locator(sel).first
        try:
            if loc.is_visible(timeout=2500):
                search = loc
                break
        except Exception:
            continue
    if search is None:
        logger.warning("[Human] Không thấy ô Tìm kiếm Facebook")
        return False
    ha.smart_click(search, label="ô tìm kiếm")
    human_pause(kind="click", label="focus search")
    time.sleep(random.uniform(0.9, 1.6))
    ha.smart_type_search(search, keyword, label="search fb", already_focused=True)
    human_pause(kind="input", label="sau Enter tìm kiếm")
    _wait_search_results_ready(page, cfg)
    return True


def module_search_reels(page: Page, *, probability: float = 0.55, cfg: HumanInteractionProfile | None = None) -> bool:
    """
    Tìm Reels qua ô Search (không vào thẳng URL /reel/).

    Gõ từ khóa xu hướng → Enter → tab Reels/Video → xem vài clip.
    """
    if random.random() > probability:
        logger.info("[Human] Bỏ qua module Tìm Reels (xác suất)")
        return False
    profile = cfg or resolve_profile("normal")
    ha = _human(page, profile)
    kw = random.choice(_REELS_KEYWORDS)
    logger.info("[Human] Module Tìm Reels qua Search: {}", kw)
    try:
        page.goto("https://www.facebook.com/", wait_until="domcontentloaded", timeout=60_000)
        _module_micro_pause(profile, label="home_before_search")
        _scroll_feed_top_to_bottom(ha, profile, short=True, like_rate=profile.like_rate_pct * 0.25)
        if not _open_search_and_type(page, ha, kw, profile):
            return False
        _scroll_feed_top_to_bottom(ha, profile, short=True)
        human_pause(kind="step", label="trước tab Reels")
        time.sleep(random.uniform(0.9, 1.6))
        reels_tab = page.locator(
            '[role="tab"]:has-text("Reels"), [role="tab"]:has-text("Video"), '
            'a[href*="reels"], span:text-is("Reels")'
        ).first
        try:
            if reels_tab.is_visible(timeout=5000):
                ha.smart_click(reels_tab, label="tab Reels")
                human_pause(label="reels_tab", kind="action")
        except PlaywrightTimeoutError:
            logger.debug("[Human] Không thấy tab Reels — thử click kết quả đầu")
            first = page.locator('[role="article"] a, [role="link"]').first
            ha.smart_click(first, label="kết quả tìm kiếm")
        _reels_clip_wait(page, profile)
        for _ in range(random.randint(1, 3)):
            page.keyboard.press("ArrowDown")
            human_pause(kind="action", label="xem reel")
            _reels_clip_wait(page, profile)
        return True
    except Exception as exc:  # noqa: BLE001
        logger.warning("[Human] Tìm Reels lỗi: {}", exc)
        return False


def module_reels_watch(page: Page, *, probability: float = 0.60, cfg: HumanInteractionProfile | None = None) -> bool:
    """Xem Reels sau khi đã vào luồng video (ArrowDown + dwell)."""
    if random.random() > probability:
        logger.info("[Human] Bỏ qua module Reels xem tiếp (xác suất)")
        return False
    profile = cfg or resolve_profile("normal")
    ha = _human(page, profile)
    logger.info("[Human] Module Reels xem tiếp")
    try:
        if "reel" not in (page.url or "").lower():
            page.goto("https://www.facebook.com/reel/", wait_until="domcontentloaded", timeout=45_000)
            _page_load_pause(profile)
        _module_micro_pause(profile, label="reels_load")
        _scroll_feed_top_to_bottom(ha, profile, short=True, like_rate=profile.like_rate_pct * 0.45)
        for _ in range(random.randint(1, 3)):
            page.keyboard.press("ArrowDown")
            human_pause(kind="action", label="reel tiếp")
            _reels_clip_wait(page, profile)
        return True
    except Exception as exc:  # noqa: BLE001
        logger.warning("[Human] Reels lỗi: {}", exc)
        return False


def module_search_fanpage(page: Page, *, probability: float = 0.40, cfg: HumanInteractionProfile | None = None) -> bool:
    """Tìm Page qua ô Search (không URL trực tiếp)."""
    if random.random() > probability:
        logger.info("[Human] Bỏ qua module Tìm Page (xác suất)")
        return False
    profile = cfg or resolve_profile("normal")
    ha = _human(page, profile)
    keywords = ("công nghệ", "du lịch", "ẩm thực", "thể thao", "âm nhạc", "phim ảnh")
    kw = random.choice(keywords)
    logger.info("[Human] Module Tìm kiếm Page: {}", kw)
    try:
        page.goto("https://www.facebook.com/", wait_until="domcontentloaded", timeout=60_000)
        _module_micro_pause(profile, label="home_search_page")
        _scroll_feed_top_to_bottom(ha, profile, short=True, like_rate=profile.like_rate_pct * 0.2)
        if not _open_search_and_type(page, ha, kw, profile):
            return False
        _scroll_feed_top_to_bottom(ha, profile, short=True)
        human_pause(kind="step", label="trước tab Pages")
        time.sleep(random.uniform(0.8, 1.5))
        pages_tab = page.locator('[role="tab"]:has-text("Pages"), [role="tab"]:has-text("Trang")').first
        try:
            if pages_tab.is_visible(timeout=4000):
                ha.smart_click(pages_tab, label="tab Pages")
        except PlaywrightTimeoutError:
            pass
        link = page.locator('[role="article"] a[href*="/"]').first
        ha.smart_click(link, label="page link")
        _module_micro_pause(profile, label="page_visit")
        _scroll_feed_top_to_bottom(ha, profile, short=False, like_rate=profile.like_rate_pct * 0.4)
        return True
    except Exception as exc:  # noqa: BLE001
        logger.warning("[Human] Tìm Page lỗi: {}", exc)
        return False


def _spin_caption() -> str:
    templates = (
        "Chuc mot ngay tot lanh.",
        "Cam on moi nguoi da theo doi.",
        "Chia se khoanh khac hom nay.",
    )
    return random.choice(templates)


def module_post_story(
    page: Page,
    *,
    probability: float = 0.20,
    media_dir: str | None = None,
    cfg: HumanInteractionProfile | None = None,
) -> bool:
    """Soạn bài nhẹ (không publish tự động)."""
    if random.random() > probability:
        logger.info("[Human] Bỏ qua module Đăng bài (xác suất)")
        return False
    profile = cfg or resolve_profile("normal")
    ha = _human(page, profile)
    logger.info("[Human] Module Đăng bài (nhẹ)")
    try:
        page.goto("https://www.facebook.com/", wait_until="domcontentloaded", timeout=60_000)
        _module_micro_pause(profile, label="composer_open")
        _scroll_feed_top_to_bottom(ha, profile, short=True)
        box = page.locator(
            '[role="textbox"][contenteditable="true"], '
            '[aria-label*="Bạn đang nghĩ" i], [aria-label*="What\'s on your mind" i]'
        ).first
        try:
            ha.smart_click(box, label="composer")
        except PlaywrightTimeoutError:
            create_btn = page.locator('[role="button"]:has-text("Tạo tin")').first
            ha.smart_click(create_btn, label="Tạo tin")
            box = page.locator('[role="textbox"][contenteditable="true"]').first
            ha.smart_click(box, label="composer")
        caption = _spin_caption()
        from src.utils.human_typing import human_type_locator

        human_type_locator(box, caption, submit_enter=False, clear_first=True, label="composer caption")
        human_pause(label="sau nhập caption", kind="input")
        logger.info("[Human] Đã soạn caption (không publish tự động): {}", caption[:40])
        return True
    except Exception as exc:  # noqa: BLE001
        logger.warning("[Human] Đăng bài lỗi: {}", exc)
        return False


def run_shuffled_interaction_modules(
    page: Page,
    *,
    profile: HumanInteractionProfile | None = None,
    on_status: StatusCallback | None = None,
) -> None:
    """Xáo trộn module và chạy với Deep Delay giữa các module đã chạy."""
    cfg = profile or resolve_profile("normal")
    modules = [
        ("newsfeed", lambda: module_newsfeed_like(page, probability=cfg.newsfeed_prob, cfg=cfg)),
        ("search_reels", lambda: module_search_reels(page, probability=cfg.reels_prob, cfg=cfg)),
        ("reels", lambda: module_reels_watch(page, probability=max(0.22, cfg.reels_prob - 0.28), cfg=cfg)),
        ("search_page", lambda: module_search_fanpage(page, probability=cfg.search_prob, cfg=cfg)),
        ("post", lambda: module_post_story(page, probability=cfg.post_prob, cfg=cfg)),
    ]
    random.shuffle(modules)
    max_mod = max(1, int(getattr(cfg, "max_modules_per_run", 3) or 3))
    ran_any = False
    success_count = 0
    for name, fn in modules:
        if success_count >= max_mod:
            logger.info("[Human] Đã đủ {} module/lượt — bỏ qua phần còn lại", max_mod)
            break
        if on_status:
            on_status("running", f"Module: {name}")
        _module_micro_pause(cfg, label=f"trước {name}")
        if fn():
            ran_any = True
            success_count += 1
            if success_count < max_mod:
                deep_delay_between_modules(
                    min_sec=cfg.deep_delay_min_sec,
                    max_sec=cfg.deep_delay_max_sec,
                )
    if not ran_any:
        logger.info("[Human] Không module nào chạy (xác suất) — scroll nhẹ")
        try:
            page.goto("https://www.facebook.com/", wait_until="domcontentloaded", timeout=45_000)
            _scroll_feed_top_to_bottom(_human(page, cfg), cfg, like_rate=cfg.like_rate_pct)
        except Exception as exc:  # noqa: BLE001
            logger.warning("[Human] Fallback scroll: {}", exc)
