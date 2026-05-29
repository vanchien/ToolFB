"""Trích followers/views từ text/DOM Insights Meta Business."""

from __future__ import annotations

import re
from typing import Any

from src.utils.page_insights_format import parse_metric_number

_FOLLOWER_LABEL_RE = re.compile(
    r"followers?|người\s*theo\s*dõi|theo\s*dõi|total\s*followers?|fan(s)?\b",
    re.I,
)
_VIEW_LABEL_RE = re.compile(
    r"(?:content\s+)?views?|video\s+views?|lượt\s*xem|view\s*count|"
    r"total\s+views?|page\s+views?|reach",
    re.I,
)
_SKIP_VIEW_RE = re.compile(r"interview|overview\s+of\s+views", re.I)

_EXTRACT_METRICS_JS = r"""
() => {
  const parseToken = (raw) => {
    const s = String(raw || '').trim().replace(/\u00a0/g, ' ');
    if (!s) return null;
    const m = s.match(/^([\d.,]+)\s*([KMB])?$/i);
    if (!m) return null;
    let num = m[1].replace(/,/g, '');
    if (num.includes('.') && num.split('.').pop().length === 3 && !m[2]) {
      num = num.replace(/\./g, '');
    }
    let base = parseFloat(num);
    if (!isFinite(base)) return null;
    const suf = (m[2] || '').toUpperCase();
    if (suf === 'K') base *= 1000;
    else if (suf === 'M') base *= 1000000;
    else if (suf === 'B') base *= 1000000000;
    return Math.round(base);
  };

  const pickNear = (labelRe, skipRe) => {
    const nodes = Array.from(document.querySelectorAll(
      'span, div, p, h1, h2, h3, h4, [role="heading"], [role="rowheader"], [role="gridcell"]'
    ));
    for (const el of nodes) {
      const t = (el.textContent || '').replace(/\s+/g, ' ').trim();
      if (!t || t.length > 120) continue;
      if (!labelRe.test(t) || (skipRe && skipRe.test(t))) continue;
      const same = t.match(/([\d.,]+\s*[KMB]?)/i);
      if (same) {
        const v = parseToken(same[1]);
        if (v != null) return v;
      }
      let sib = el.nextElementSibling;
      for (let i = 0; i < 4 && sib; i++, sib = sib.nextElementSibling) {
        const st = (sib.textContent || '').trim();
        const m = st.match(/^([\d.,]+\s*[KMB]?)$/i);
        if (m) {
          const v = parseToken(m[1]);
          if (v != null) return v;
        }
      }
      let p = el.parentElement;
      for (let d = 0; d < 3 && p; d++, p = p.parentElement) {
        const pt = (p.textContent || '').replace(/\s+/g, ' ').trim();
        if (pt.length > 200) continue;
        const nums = [...pt.matchAll(/([\d.,]+\s*[KMB]?)/gi)].map(x => parseToken(x[1])).filter(v => v != null);
        if (nums.length === 1) return nums[0];
        if (nums.length > 1) return nums[0];
      }
    }
    return null;
  };

  const body = (document.body && document.body.innerText) || '';
  const lines = body.split(/\n+/).map(x => x.trim()).filter(Boolean);
  const scanLines = (labelRe, skipRe) => {
    for (let i = 0; i < lines.length; i++) {
      const line = lines[i];
      if (!labelRe.test(line) || (skipRe && skipRe.test(line))) continue;
      const inline = line.match(/([\d.,]+\s*[KMB]?)/i);
      if (inline) {
        const v = parseToken(inline[1]);
        if (v != null) return v;
      }
      for (let j = i + 1; j < Math.min(lines.length, i + 4); j++) {
        const m = lines[j].match(/^([\d.,]+\s*[KMB]?)$/i);
        if (m) {
          const v = parseToken(m[1]);
          if (v != null) return v;
        }
      }
    }
    return null;
  };

  const followers =
    pickNear(/followers?|người theo dõi|theo dõi|total followers/i, null) ||
    scanLines(/followers?|người theo dõi|theo dõi/i, null);
  const views =
    pickNear(/content views?|video views?|views?|lượt xem|page views?/i, /interview/i) ||
    scanLines(/content views?|video views?|views?|lượt xem/i, /interview/i);

  return { followers, views, url: location.href || '' };
}
"""


def merge_metrics(
    *parts: dict[str, Any],
) -> tuple[int | None, int | None]:
    followers: int | None = None
    views: int | None = None
    for p in parts:
        if not isinstance(p, dict):
            continue
        f = p.get("followers")
        v = p.get("views")
        if followers is None and f is not None:
            followers = int(f) if isinstance(f, (int, float)) else parse_metric_number(str(f))
        if views is None and v is not None:
            views = int(v) if isinstance(v, (int, float)) else parse_metric_number(str(v))
    return followers, views


def metrics_from_body_text(body: str) -> tuple[int | None, int | None]:
    """Fallback thuần Python khi evaluate thất bại."""
    lines = [ln.strip() for ln in body.splitlines() if ln.strip()]
    followers: int | None = None
    views: int | None = None
    for i, line in enumerate(lines):
        if _FOLLOWER_LABEL_RE.search(line) and followers is None:
            for candidate in (line, *(lines[i + 1 : i + 4])):
                m = re.search(r"([\d.,]+\s*[KMB]?)", candidate, re.I)
                if m:
                    followers = parse_metric_number(m.group(1))
                    if followers is not None:
                        break
        if _VIEW_LABEL_RE.search(line) and not _SKIP_VIEW_RE.search(line) and views is None:
            for candidate in (line, *(lines[i + 1 : i + 4])):
                m = re.search(r"([\d.,]+\s*[KMB]?)", candidate, re.I)
                if m:
                    views = parse_metric_number(m.group(1))
                    if views is not None:
                        break
    return followers, views
