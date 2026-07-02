"""
Các module tương tác giống người dùng thật trên Facebook.

Dùng ``HumanAction`` (chuột Bezier, cuộn tự nhiên, gõ có typo) thay cho click/scroll thô.
"""

from __future__ import annotations

import random
import time
from collections.abc import Callable
from typing import Any

from loguru import logger
from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError

from src.automation.browser_factory import is_playwright_target_closed_error

from src.automation.facebook_actions import human_pause
from src.services.human_ai_comment import generate_facebook_comment
from src.services.human_interaction_profile import HumanInteractionProfile, resolve_profile
from src.utils.human_action import HumanAction

StatusCallback = Callable[[str, str], None]
StopCallback = Callable[[], bool]


def _page_usable(page: Page) -> bool:
    try:
        return page.is_closed() is False
    except Exception:
        return False


def _raise_if_browser_closed(page: Page) -> None:
    if not _page_usable(page):
        raise RuntimeError("Target page, context or browser has been closed")


def _re_raise_browser_closed(exc: BaseException) -> None:
    if is_playwright_target_closed_error(exc):
        raise exc


def _interruptible_sleep(
    seconds: float,
    *,
    page: Page | None = None,
    should_stop: StopCallback | None = None,
) -> bool:
    """Ngủ có thể ngắt bởi «Dừng». Trả ``False`` nếu người dùng đã dừng pool."""
    end = time.monotonic() + max(0.0, float(seconds))
    while time.monotonic() < end:
        if should_stop and should_stop():
            return False
        chunk = min(0.4, end - time.monotonic())
        if chunk <= 0:
            break
        ms = max(50, int(chunk * 1000))
        if page is not None and _page_usable(page):
            try:
                page.wait_for_timeout(ms)
            except Exception as exc:  # noqa: BLE001
                _re_raise_browser_closed(exc)
                time.sleep(chunk)
        else:
            time.sleep(chunk)
    return not bool(should_stop and should_stop())

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


def deep_delay_between_modules(
    *,
    min_sec: float,
    max_sec: float,
    should_stop: StopCallback | None = None,
) -> None:
    """Nghỉ sâu giữa các module lớn — có thể ngắt bởi «Dừng»."""
    lo = max(0.1, float(min_sec))
    hi = max(lo, float(max_sec))
    sec = random.uniform(lo, hi)
    logger.info("[Human] Deep delay {:.1f}s giữa các module", sec)
    _interruptible_sleep(sec, should_stop=should_stop)


def _module_micro_pause(
    cfg: HumanInteractionProfile,
    *,
    label: str = "",
    should_stop: StopCallback | None = None,
) -> bool:
    """Tạm dừng ngắn trước/sau thao tác trong một module. Trả ``False`` nếu đã bấm Dừng."""
    if should_stop and should_stop():
        return False
    human_pause(kind="action", label=label or "module")
    sec = random.uniform(cfg.module_pause_min_sec, cfg.module_pause_max_sec)
    return _interruptible_sleep(sec, should_stop=should_stop)


def _page_load_pause(
    cfg: HumanInteractionProfile,
    *,
    should_stop: StopCallback | None = None,
) -> bool:
    """Chờ ngắn sau goto — theo preset. Trả ``False`` nếu đã bấm Dừng."""
    sec = random.uniform(cfg.page_load_pause_min_sec, cfg.page_load_pause_max_sec)
    return _interruptible_sleep(sec, should_stop=should_stop)


def _interruptible_step_pause(
    *,
    label: str = "",
    should_stop: StopCallback | None = None,
) -> bool:
    """``human_pause(kind=step)`` có thể ngắt — thay cho ``time.sleep`` cố định giữa bước."""
    if should_stop and should_stop():
        return False
    human_pause(kind="step", label=label)
    return _interruptible_sleep(random.uniform(0.4, 1.2), should_stop=should_stop)


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
    should_stop: StopCallback | None = None,
) -> bool:
    """Cuộn từ đầu trang xuống — số vòng theo preset (safe/normal/fast)."""
    if should_stop and should_stop():
        return False
    if short:
        lo, hi = cfg.scroll_rounds_short_min, cfg.scroll_rounds_short_max
    else:
        lo, hi = cfg.scroll_rounds_min, cfg.scroll_rounds_max
    rounds = random.randint(max(4, lo), max(lo, hi))
    ok = ha.natural_scroll_feed(
        rounds=rounds,
        like_rate=like_rate,
        comment_rate=comment_rate,
        on_like=on_like,
        on_comment=on_comment,
        downward_bias=0.97,
        scroll_from_top=True,
        dwell_scale=cfg.dwell_scale,
        should_stop=should_stop,
    )
    if not ok or (should_stop and should_stop()):
        return False
    return _interruptible_step_pause(label="sau cuộn feed", should_stop=should_stop)


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


def _clear_feed_like_tags(page: Page) -> None:
    """Xóa marker tạm sau thao tác Like."""
    try:
        page.evaluate(
            """() => {
                document.querySelectorAll('[data-toolfb-like-btn]').forEach(el => {
                    el.removeAttribute('data-toolfb-like-btn');
                });
                document.querySelectorAll('[data-toolfb-feed-article]').forEach(el => {
                    el.removeAttribute('data-toolfb-feed-article');
                });
            }"""
        )
    except Exception:
        pass


def _tag_visible_post_like_target(page: Page) -> bool:
    """
    Đánh dấu nút Like của **bài chính** đang hiển thị giữa feed.

    Bỏ qua bài lồng nhau (comment), sidebar và nút Like bình luận.
    """
    try:
        return bool(
            page.evaluate(
                """() => {
                    document.querySelectorAll('[data-toolfb-like-btn]').forEach(el => {
                        el.removeAttribute('data-toolfb-like-btn');
                    });
                    document.querySelectorAll('[data-toolfb-feed-article]').forEach(el => {
                        el.removeAttribute('data-toolfb-feed-article');
                    });
                    const isTopLevelArticle = (el) => {
                        let p = el.parentElement;
                        while (p) {
                            if (p !== el && p.getAttribute('role') === 'article') return false;
                            p = p.parentElement;
                        }
                        return true;
                    };
                    const likeLabelOk = (label) => {
                        const s = (label || '').trim().toLowerCase();
                        if (!s) return false;
                        if (s.includes('comment') || s.includes('bình luận')) return false;
                        return s === 'like' || s === 'thích'
                            || s.startsWith('like ') || s.startsWith('thích ');
                    };
                    const arts = [...document.querySelectorAll(
                        '[role="main"] [role="article"], [role="feed"] [role="article"]'
                    )].filter(isTopLevelArticle).slice(0, 20);
                    const vh = window.innerHeight;
                    const vmid = vh * 0.45;
                    let bestArt = null, bestScore = -1;
                    for (const el of arts) {
                        const r = el.getBoundingClientRect();
                        if (r.height < 48 || r.bottom < 80 || r.top > vh - 40) continue;
                        const visTop = Math.max(0, r.top);
                        const visBot = Math.min(vh, r.bottom);
                        const area = Math.max(0, visBot - visTop) * Math.max(0, r.width);
                        const centerDist = Math.abs((r.top + r.bottom) / 2 - vmid);
                        const score = area - centerDist * 3;
                        if (score > bestScore) { bestScore = score; bestArt = el; }
                    }
                    if (!bestArt) return false;
                    bestArt.setAttribute('data-toolfb-feed-article', '1');
                    const ar = bestArt.getBoundingClientRect();
                    const footerY = ar.top + ar.height * 0.32;
                    const buttons = [...bestArt.querySelectorAll('[role="button"][aria-label]')]
                        .filter(btn => {
                            if (btn.getAttribute('aria-pressed') === 'true') return false;
                            if (!likeLabelOk(btn.getAttribute('aria-label'))) return false;
                            const br = btn.getBoundingClientRect();
                            if (br.width < 18 || br.height < 18) return false;
                            if (br.bottom < 80 || br.top > vh - 30) return false;
                            return br.top >= footerY;
                        });
                    if (!buttons.length) {
                        const fallback = [...bestArt.querySelectorAll('[role="button"][aria-label]')]
                            .filter(btn => {
                                if (btn.getAttribute('aria-pressed') === 'true') return false;
                                return likeLabelOk(btn.getAttribute('aria-label'));
                            });
                        if (!fallback.length) return false;
                        fallback[0].setAttribute('data-toolfb-like-btn', '1');
                        return true;
                    }
                    buttons.sort((a, b) => a.getBoundingClientRect().top - b.getBoundingClientRect().top);
                    buttons[0].setAttribute('data-toolfb-like-btn', '1');
                    return true;
                }"""
            )
        )
    except Exception as exc:  # noqa: BLE001
        logger.debug("[Human] tag like target: {}", exc)
        return False


def _article_most_visible_in_viewport(page: Page) -> Any | None:
    """Bài feed chính trong viewport — dùng marker DOM đồng bộ với JS chọn Like."""
    if not _tag_visible_post_like_target(page):
        return None
    loc = page.locator('[data-toolfb-feed-article="1"]').first
    try:
        if loc.is_visible(timeout=1200):
            return loc
    except Exception:
        pass
    return None


def _locate_like_in_article(article: Any) -> Any | None:
    """Nút Like/Thích trong một bài — bỏ qua đã thích (``aria-pressed=true``)."""
    loc = article.page.locator('[data-toolfb-like-btn="1"]').first
    try:
        if loc.is_visible(timeout=900):
            pressed = loc.get_attribute("aria-pressed")
            if pressed and str(pressed).lower() == "true":
                return None
            return loc
    except Exception:
        pass
    selectors = (
        '[role="button"][aria-label="Like"]:not([aria-pressed="true"])',
        '[role="button"][aria-label="Thích"]:not([aria-pressed="true"])',
        '[role="button"][aria-label^="Like " i]:not([aria-pressed="true"])',
        '[role="button"][aria-label^="Thích " i]:not([aria-pressed="true"])',
    )
    for sel in selectors:
        btn = article.locator(sel).first
        try:
            if btn.is_visible(timeout=500):
                return btn
        except Exception:
            continue
    return None


def _try_like_visible_post(page: Page, ha: HumanAction) -> bool:
    """Like bài đang hiển thị giữa feed — không Like sidebar / comment."""
    try:
        if not _tag_visible_post_like_target(page):
            logger.debug("[Human] Không tìm được nút Like bài chính trong viewport")
            return False
        like = page.locator('[data-toolfb-like-btn="1"]').first
        if not like.is_visible(timeout=1500):
            logger.debug("[Human] Nút Like đã tag không còn visible")
            return False
        pressed = like.get_attribute("aria-pressed")
        if pressed and str(pressed).lower() == "true":
            logger.debug("[Human] Bài đã Like — bỏ qua")
            return False
        like.scroll_into_view_if_needed(timeout=4000)
        try:
            like.click(delay=random.randint(55, 180), timeout=6000)
            human_pause(kind="click", label="like bài")
            pressed_after = like.get_attribute("aria-pressed")
            if pressed_after and str(pressed_after).lower() == "true":
                logger.info("[Human] Đã Like bài trong viewport (xác nhận aria-pressed)")
                return True
            if ha.smart_click(like, label="like bài viewport (fallback)"):
                human_pause(kind="click", label="like bài")
                logger.info("[Human] Đã Like bài trong viewport")
                return True
        except PlaywrightTimeoutError:
            pass
    except PlaywrightTimeoutError:
        pass
    except Exception as exc:  # noqa: BLE001
        _re_raise_browser_closed(exc)
        logger.debug("[Human] Like viewport: {}", exc)
    finally:
        _clear_feed_like_tags(page)
    return False


def _try_comment_visible_post(page: Page, ha: HumanAction, *, use_ai: bool) -> None:
    article = _article_most_visible_in_viewport(page)
    if article is None:
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


def module_newsfeed_like(
    page: Page,
    *,
    probability: float = 0.70,
    cfg: HumanInteractionProfile | None = None,
    should_stop: StopCallback | None = None,
) -> bool:
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
        if not _module_micro_pause(profile, label="newsfeed_load", should_stop=should_stop):
            return False
        if not _page_load_pause(profile, should_stop=should_stop):
            return False

        def on_like(p: Page) -> None:
            if should_stop and should_stop():
                return
            _try_like_visible_post(p, _human(p, profile))

        def on_comment(p: Page) -> None:
            if should_stop and should_stop():
                return
            if profile.ai_comments:
                _try_comment_visible_post(p, _human(p, profile), use_ai=True)

        if not _scroll_feed_top_to_bottom(
            ha,
            profile,
            like_rate=profile.like_rate_pct,
            comment_rate=profile.comment_rate_pct if profile.ai_comments else 0.0,
            on_like=on_like,
            on_comment=on_comment if profile.ai_comments else None,
            should_stop=should_stop,
        ):
            return False
        if should_stop and should_stop():
            return False
        if not _interruptible_step_pause(label="cuối newsfeed", should_stop=should_stop):
            return False
        return True
    except Exception as exc:  # noqa: BLE001
        _re_raise_browser_closed(exc)
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


def module_search_reels(
    page: Page,
    *,
    probability: float = 0.55,
    cfg: HumanInteractionProfile | None = None,
    should_stop: StopCallback | None = None,
) -> bool:
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
        _scroll_feed_top_to_bottom(
            ha, profile, short=True, like_rate=profile.like_rate_pct * 0.25, should_stop=should_stop
        )
        if should_stop and should_stop():
            return False
        if not _open_search_and_type(page, ha, kw, profile):
            return False
        _scroll_feed_top_to_bottom(ha, profile, short=True, should_stop=should_stop)
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
        _re_raise_browser_closed(exc)
        logger.warning("[Human] Tìm Reels lỗi: {}", exc)
        return False


def module_reels_watch(
    page: Page,
    *,
    probability: float = 0.60,
    cfg: HumanInteractionProfile | None = None,
    should_stop: StopCallback | None = None,
) -> bool:
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
        _scroll_feed_top_to_bottom(
            ha, profile, short=True, like_rate=profile.like_rate_pct * 0.45, should_stop=should_stop
        )
        for _ in range(random.randint(1, 3)):
            page.keyboard.press("ArrowDown")
            human_pause(kind="action", label="reel tiếp")
            _reels_clip_wait(page, profile)
        return True
    except Exception as exc:  # noqa: BLE001
        _re_raise_browser_closed(exc)
        logger.warning("[Human] Reels lỗi: {}", exc)
        return False


def module_search_fanpage(
    page: Page,
    *,
    probability: float = 0.40,
    cfg: HumanInteractionProfile | None = None,
    should_stop: StopCallback | None = None,
) -> bool:
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
        _scroll_feed_top_to_bottom(
            ha, profile, short=True, like_rate=profile.like_rate_pct * 0.2, should_stop=should_stop
        )
        if not _open_search_and_type(page, ha, kw, profile):
            return False
        _scroll_feed_top_to_bottom(ha, profile, short=True, should_stop=should_stop)
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
        _scroll_feed_top_to_bottom(
            ha, profile, short=False, like_rate=profile.like_rate_pct * 0.4, should_stop=should_stop
        )
        return True
    except Exception as exc:  # noqa: BLE001
        _re_raise_browser_closed(exc)
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
    should_stop: StopCallback | None = None,
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
        _scroll_feed_top_to_bottom(ha, profile, short=True, should_stop=should_stop)
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
        _re_raise_browser_closed(exc)
        logger.warning("[Human] Đăng bài lỗi: {}", exc)
        return False


def run_shuffled_interaction_modules(
    page: Page,
    *,
    profile: HumanInteractionProfile | None = None,
    on_status: StatusCallback | None = None,
    should_stop: StopCallback | None = None,
) -> None:
    """Xáo trộn module và chạy với Deep Delay giữa các module đã chạy."""
    cfg = profile or resolve_profile("normal")
    modules = [
        ("newsfeed", lambda: module_newsfeed_like(page, probability=cfg.newsfeed_prob, cfg=cfg, should_stop=should_stop)),
        ("search_reels", lambda: module_search_reels(page, probability=cfg.reels_prob, cfg=cfg, should_stop=should_stop)),
        ("reels", lambda: module_reels_watch(page, probability=max(0.22, cfg.reels_prob - 0.28), cfg=cfg, should_stop=should_stop)),
        ("search_page", lambda: module_search_fanpage(page, probability=cfg.search_prob, cfg=cfg, should_stop=should_stop)),
        ("post", lambda: module_post_story(page, probability=cfg.post_prob, cfg=cfg, should_stop=should_stop)),
    ]
    random.shuffle(modules)
    max_mod = max(1, int(getattr(cfg, "max_modules_per_run", 3) or 3))
    ran_any = False
    success_count = 0
    for name, fn in modules:
        if should_stop and should_stop():
            logger.info("[Human] Dừng tương tác — người dùng bấm Dừng (trước module {})", name)
            break
        if success_count >= max_mod:
            logger.info("[Human] Đã đủ {} module/lượt — bỏ qua phần còn lại", max_mod)
            break
        _raise_if_browser_closed(page)
        if on_status:
            on_status("running", f"Module: {name}")
        _module_micro_pause(cfg, label=f"trước {name}")
        try:
            if fn():
                ran_any = True
                success_count += 1
                if success_count < max_mod:
                    deep_delay_between_modules(
                        min_sec=cfg.deep_delay_min_sec,
                        max_sec=cfg.deep_delay_max_sec,
                        should_stop=should_stop,
                    )
            elif should_stop and should_stop():
                logger.info("[Human] Dừng tương tác — người dùng bấm Dừng (sau module {})", name)
                break
        except Exception as exc:  # noqa: BLE001
            if is_playwright_target_closed_error(exc) or not _page_usable(page):
                raise
            logger.warning("[Human] Module {} lỗi (bỏ qua): {}", name, exc)
    if not ran_any:
        logger.info("[Human] Không module nào chạy (xác suất) — scroll nhẹ")
        try:
            _raise_if_browser_closed(page)
            page.goto("https://www.facebook.com/", wait_until="domcontentloaded", timeout=45_000)
            _scroll_feed_top_to_bottom(
                _human(page, cfg), cfg, like_rate=cfg.like_rate_pct, should_stop=should_stop
            )
        except Exception as exc:  # noqa: BLE001
            if is_playwright_target_closed_error(exc):
                raise
            logger.warning("[Human] Fallback scroll: {}", exc)
