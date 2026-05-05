"""Biểu thức FFmpeg cho vị trí đổi theo thời gian (seed + nhảy ô hoặc nội suy mượt)."""

from __future__ import annotations


def motion_coeffs(seed: int) -> tuple[int, int, int, int]:
    """
    (mul_x, mul_y, add_x, add_y) cho mod(k*mul+add, range).
    seed == 0 giữ đúng hệ số cũ (44001, 79001, 0, 0) để dự án cũ không đổi quỹ đạo.
    """
    if int(seed) == 0:
        return 44001, 79001, 0, 0
    s = abs(int(seed)) % 2000000000
    mul_x = 40009 + (s % 90001) * 4
    mul_y = 60007 + ((s // 90001) % 90001) * 4
    add_x = (s * 1103515245 + 12345) % 1000003
    add_y = (s * 134775813 + 1) % 1000033
    return mul_x, mul_y, add_x, add_y


def _clamp_interval(interval_sec: float) -> float:
    return max(0.25, min(120.0, float(interval_sec)))


def overlay_random_xy_expr(
    interval_sec: float,
    *,
    seed: int = 0,
    smooth: bool = False,
) -> tuple[str, str]:
    """
    (x_expr, y_expr) cho filter overlay.
    smooth: nội suy tuyến tính giữa điểm k và k+1 trong mỗi bước (dùng st/ld).
    """
    i = _clamp_interval(interval_sec)
    ifs = f"{i:.6f}"
    mx, my, ax, ay = motion_coeffs(seed)

    if not smooth:
        x = f"max(0\\,mod(floor(t/{ifs})*{mx}+{ax}\\,max(1\\,main_w-overlay_w)))"
        y = f"max(0\\,mod(floor(t/{ifs})*{my}+{ay}\\,max(1\\,main_h-overlay_h)))"
        return x, y

    # k=floor(t/i); Wx=max(1,main_w-overlay_w); x0,x1; u=clamp((t-k*i)/i,0,~1); lerp
    # Trong filter_complex, `;` tách filter — phải dùng `\;` giữa các st(...).
    x = (
        f"st(0\\,floor(t/{ifs}))\\;st(1\\,max(1\\,main_w-overlay_w))\\;"
        f"st(2\\,mod(ld(0)*{mx}+{ax}\\,ld(1)))\\;st(3\\,mod((ld(0)+1)*{mx}+{ax}\\,ld(1)))\\;"
        f"max(0\\,ld(2)+min(max(0\\,(t-ld(0)*{ifs})/{ifs})\\,0.999999)*(ld(3)-ld(2)))"
    )
    y = (
        f"st(0\\,floor(t/{ifs}))\\;st(1\\,max(1\\,main_h-overlay_h))\\;"
        f"st(2\\,mod(ld(0)*{my}+{ay}\\,ld(1)))\\;st(3\\,mod((ld(0)+1)*{my}+{ay}\\,ld(1)))\\;"
        f"max(0\\,ld(2)+min(max(0\\,(t-ld(0)*{ifs})/{ifs})\\,0.999999)*(ld(3)-ld(2)))"
    )
    return x, y


def drawtext_random_xy_expr(
    interval_sec: float,
    *,
    seed: int = 0,
    smooth: bool = False,
) -> tuple[str, str]:
    """Biểu thức x,y cho drawtext (w, h, text_w, text_h)."""
    i = _clamp_interval(interval_sec)
    ifs = f"{i:.6f}"
    mx, my, ax, ay = motion_coeffs(seed)

    if not smooth:
        x = f"max(0\\,mod(floor(t/{ifs})*{mx}+{ax}\\,max(1\\,w-text_w)))"
        y = f"max(0\\,mod(floor(t/{ifs})*{my}+{ay}\\,max(1\\,h-text_h)))"
        return x, y

    x = (
        f"st(0\\,floor(t/{ifs}))\\;st(1\\,max(1\\,w-text_w))\\;"
        f"st(2\\,mod(ld(0)*{mx}+{ax}\\,ld(1)))\\;st(3\\,mod((ld(0)+1)*{mx}+{ax}\\,ld(1)))\\;"
        f"max(0\\,ld(2)+min(max(0\\,(t-ld(0)*{ifs})/{ifs})\\,0.999999)*(ld(3)-ld(2)))"
    )
    y = (
        f"st(0\\,floor(t/{ifs}))\\;st(1\\,max(1\\,h-text_h))\\;"
        f"st(2\\,mod(ld(0)*{my}+{ay}\\,ld(1)))\\;st(3\\,mod((ld(0)+1)*{my}+{ay}\\,ld(1)))\\;"
        f"max(0\\,ld(2)+min(max(0\\,(t-ld(0)*{ifs})/{ifs})\\,0.999999)*(ld(3)-ld(2)))"
    )
    return x, y
