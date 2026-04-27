"""
Viewport cho Facebook automation.

- **Desktop hẹp** (vd ``450×400``): giao diện ``www.facebook.com`` **không** co responsive — sẽ vỡ layout.
  Khi ``FB_VIEWPORT_WIDTH`` < ``FB_DESKTOP_SAFE_MIN_WIDTH`` (mặc định 900), tool tự bật
  ``use_mobile_facebook_shell`` → ``m.facebook.com`` + UA/mobile shell, vẫn giữ đúng kích thước cửa sổ.
  Tắt hành vi này: ``FB_AUTO_MOBILE_WEB_WHEN_NARROW=0`` (chấp nhận layout desktop bị cắt).

- **Desktop rộng** (``>=`` ngưỡng): ``www.facebook.com``, DPR 1.

- **Mobile + preset** (tùy chọn): ``FB_MOBILE_MODE=1``, ``FB_MOBILE_AUTO_VIEWPORT=1`` → ``playwright.devices``.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any, Optional

from loguru import logger
from playwright.sync_api import Playwright

# Mặc định cho mobile preset/compact.
_DEFAULT_MOBILE_VIEWPORT_W = 450
_DEFAULT_MOBILE_VIEWPORT_H = 400
_DEFAULT_DESKTOP_VIEWPORT_W = 1280
_DEFAULT_DESKTOP_VIEWPORT_H = 900


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    return raw.lower() not in {"0", "false", "off", "no"}


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(float(raw))
    except (TypeError, ValueError):
        return default


def _user_agent_candidates() -> str:
    return (
        os.environ.get("PLAYWRIGHT_USER_AGENT", "").strip()
        or os.environ.get("FB_MOBILE_UA", "").strip()
        or ""
    )


def _infer_device_name_from_ua(ua: str, devices: dict[str, Any]) -> str:
    """Chọn tên preset Playwright gần đúng UA (chỉ máy dọc, bỏ *landscape*)."""
    u = ua.strip()
    ul = u.lower()
    if not u:
        return "iPhone 12"

    if "pixel" in ul:
        for name in ("Pixel 7", "Pixel 5", "Pixel 4", "Pixel 3"):
            if name in devices and "landscape" not in name.lower():
                return name

    if "iphone" in ul or "ipad" in ul:
        m = re.search(r"cpu iphone os (\d+)", ul)
        if not m:
            m = re.search(r"iphone os (\d+)[._]", ul)
        if m:
            ver = int(m.group(1))
            if ver >= 18:
                pick = "iPhone 15"
            elif ver >= 17:
                pick = "iPhone 15"
            elif ver >= 16:
                pick = "iPhone 14"
            elif ver >= 15:
                pick = "iPhone 13"
            else:
                pick = "iPhone 12"
            if pick in devices:
                return pick
        if "iphone 15" in ul and "iPhone 15" in devices:
            return "iPhone 15"
        if "iphone 14" in ul and "iPhone 14" in devices:
            return "iPhone 14"
        return "iPhone 12"

    if "android" in ul:
        for name in ("Pixel 7", "Pixel 5", "Galaxy S8"):
            if name in devices and "landscape" not in name.lower():
                return name

    return "iPhone 12"


def _pick_device_descriptor(playwright: Playwright) -> tuple[str, dict[str, Any]]:
    devices: dict[str, Any] = playwright.devices
    explicit = os.environ.get("FB_MOBILE_DEVICE", "").strip()
    if explicit:
        if explicit not in devices:
            logger.warning(
                "FB_MOBILE_DEVICE={!r} không có trong Playwright — dùng iPhone 12. "
                "Gõ đúng tên preset (vd: iPhone 15, Pixel 7).",
                explicit,
            )
            name = "iPhone 12"
        else:
            name = explicit
    else:
        name = _infer_device_name_from_ua(_user_agent_candidates(), devices)
    desc = devices.get(name) or devices["iPhone 12"]
    return name, desc


@dataclass(frozen=True)
class MobileViewportResolution:
    width: int
    height: int
    device_scale_factor: float
    device_name: str
    user_agent: str | None  # None = giữ nguyên UA do caller quyết định
    from_playwright_device: bool
    #: True → điều hướng ``m.facebook`` + ``is_mobile``/UA mobile (viewport hẹp hoặc mobile mode).
    use_mobile_facebook_shell: bool = False


def resolve_mobile_viewport(
    playwright: Playwright | None,
    *,
    mobile_mode: bool,
) -> MobileViewportResolution:
    """
    Quyết định viewport / DPR / UA.

    - Mobile + ``FB_MOBILE_AUTO_VIEWPORT=1`` + không đặt cả ``FB_VIEWPORT_WIDTH`` và ``FB_VIEWPORT_HEIGHT``
      trong môi trường → lấy preset ``playwright.devices``.
    - Nếu đặt một trong hai kích thước → vẫn dùng số từ env (fallback còn lại từ preset hoặc mặc định).
    - ``FB_DEVICE_SCALE_FACTOR`` trong env luôn được ưu tiên hơn preset khi đã set.
    """
    vp_min_w = max(200, _env_int("FB_VIEWPORT_MIN_WIDTH", 280))
    vp_min_h = max(160, _env_int("FB_VIEWPORT_MIN_HEIGHT", 180))
    # Khi FB_MOBILE_MODE=0, mặc định phải là desktop đủ rộng để không tự rơi vào m.facebook.
    desktop_w = max(vp_min_w, _env_int("FB_VIEWPORT_WIDTH", _DEFAULT_DESKTOP_VIEWPORT_W))
    desktop_h = max(vp_min_h, _env_int("FB_VIEWPORT_HEIGHT", _DEFAULT_DESKTOP_VIEWPORT_H))

    if not mobile_mode:
        safe_w = max(320, _env_int("FB_DESKTOP_SAFE_MIN_WIDTH", 900))
        auto_narrow = _env_bool("FB_AUTO_MOBILE_WEB_WHEN_NARROW", True)
        use_shell = bool(auto_narrow and desktop_w < safe_w)
        if use_shell:
            logger.info(
                "Viewport {}px < {}px — Facebook desktop không responsive tại độ rộng này; "
                "dùng mobile web (m.facebook) + shell mobile, giữ cửa sổ {}×{}.",
                desktop_w,
                safe_w,
                desktop_w,
                desktop_h,
            )
        dsf = (
            float(max(2, _env_int("FB_DEVICE_SCALE_FACTOR", 3)))
            if use_shell
            else float(max(1, _env_int("FB_DEVICE_SCALE_FACTOR", 1)))
        )
        return MobileViewportResolution(
            width=desktop_w,
            height=desktop_h,
            device_scale_factor=dsf,
            device_name="(compact-m.facebook)" if use_shell else "(desktop)",
            user_agent=None,
            from_playwright_device=False,
            use_mobile_facebook_shell=use_shell,
        )

    auto = _env_bool("FB_MOBILE_AUTO_VIEWPORT", False)
    w_set = "FB_VIEWPORT_WIDTH" in os.environ and os.environ.get("FB_VIEWPORT_WIDTH", "").strip() != ""
    h_set = "FB_VIEWPORT_HEIGHT" in os.environ and os.environ.get("FB_VIEWPORT_HEIGHT", "").strip() != ""
    # Chỉ dùng preset thiết bị đầy đủ khi không ghi đè kích thước (một trong hai env là coi như thủ công).
    use_device = auto and playwright is not None and not w_set and not h_set

    if use_device:
        name, desc = _pick_device_descriptor(playwright)
        vp = desc.get("viewport") or {}
        w = int(vp.get("width", 390))
        h = int(vp.get("height", 664))
        dsf = float(desc.get("device_scale_factor", 3))
        if "FB_DEVICE_SCALE_FACTOR" in os.environ and os.environ.get("FB_DEVICE_SCALE_FACTOR", "").strip():
            dsf = float(max(1, _env_int("FB_DEVICE_SCALE_FACTOR", int(dsf))))
        w = max(vp_min_w, w)
        h = max(vp_min_h, h)
        sync_ua = _env_bool("FB_MOBILE_SYNC_UA", True)
        ua_out: str | None = None
        if sync_ua and not _user_agent_candidates():
            ua_out = str(desc.get("user_agent", "")).strip() or None
        logger.info(
            "Viewport mobile tự động theo preset Playwright: device={} | {}x{} | dpr={}",
            name,
            w,
            h,
            dsf,
        )
        return MobileViewportResolution(
            width=w,
            height=h,
            device_scale_factor=dsf,
            device_name=name,
            user_agent=ua_out,
            from_playwright_device=True,
        )

    # Thủ công / một phần env — vẫn có thể đồng bộ UA theo preset khi user không gõ PLAYWRIGHT_USER_AGENT/FB_MOBILE_UA.
    w = max(vp_min_w, _env_int("FB_VIEWPORT_WIDTH", _DEFAULT_MOBILE_VIEWPORT_W))
    h = max(vp_min_h, _env_int("FB_VIEWPORT_HEIGHT", _DEFAULT_MOBILE_VIEWPORT_H))
    dsf = float(max(1, _env_int("FB_DEVICE_SCALE_FACTOR", 3)))
    dev_name = "(env)"
    ua_out: str | None = None
    if playwright is not None and _env_bool("FB_MOBILE_SYNC_UA", True) and not _user_agent_candidates():
        dev_name, desc = _pick_device_descriptor(playwright)
        ua_out = str(desc.get("user_agent", "")).strip() or None
        logger.info(
            "Viewport mobile thủ công — đồng bộ UA theo preset: device={} | {}x{} | dpr={}",
            dev_name,
            w,
            h,
            dsf,
        )
    return MobileViewportResolution(
        width=w,
        height=h,
        device_scale_factor=dsf,
        device_name=dev_name,
        user_agent=ua_out,
        from_playwright_device=False,
    )
