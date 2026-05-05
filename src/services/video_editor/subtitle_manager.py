"""Subtitle: SRT/VTT import, thêm tay, xuất ASS cho FFmpeg."""

from __future__ import annotations

import re
import uuid
from pathlib import Path
from typing import Any


def _now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _parse_timestamp(s: str) -> float:
    s = s.strip()
    # 00:01:02,500 hoặc 00:01:02.500
    m = re.match(
        r"^(?P<h>\d+):(?P<m>\d\d):(?P<s>\d\d)(?:[,.](?P<ms>\d{1,3}))?$",
        s,
    )
    if not m:
        return 0.0
    h, mn, sec = int(m.group("h")), int(m.group("m")), int(m.group("s"))
    ms = m.group("ms") or "0"
    ms = int(ms.ljust(3, "0")[:3])
    return h * 3600 + mn * 60 + sec + ms / 1000.0


def _ass_escape(text: str) -> str:
    return text.replace("\\", "\\\\").replace("{", "\\{").replace("}", "\\}")


class SubtitleManager:
    def import_srt(self, project: dict[str, Any], srt_path: str) -> dict[str, Any]:
        raw = Path(srt_path).expanduser().read_text(encoding="utf-8", errors="replace")
        blocks = re.split(r"\n\s*\n", raw.strip())
        subs: list[dict[str, Any]] = list(project.get("subtitles") or [])
        for block in blocks:
            lines = [ln.strip() for ln in block.splitlines() if ln.strip()]
            if len(lines) < 2:
                continue
            # bỏ index nếu có
            if re.match(r"^\d+$", lines[0]):
                lines = lines[1:]
            if not lines:
                continue
            time_line = lines[0]
            if "-->" not in time_line:
                continue
            left, right = time_line.split("-->", 1)
            st = _parse_timestamp(left)
            en = _parse_timestamp(right.split()[0] if right.split() else right)
            text = "\n".join(lines[1:])
            subs.append(
                {
                    "id": f"sub_{uuid.uuid4().hex[:10]}",
                    "start": round(st, 4),
                    "end": round(en, 4),
                    "text": text,
                    "style": {
                        "font_size": 42,
                        "color": "white",
                        "outline_color": "black",
                        "outline_width": 2,
                        "position": "bottom",
                    },
                }
            )
        project["subtitles"] = subs
        return project

    def import_vtt(self, project: dict[str, Any], vtt_path: str) -> dict[str, Any]:
        raw = Path(vtt_path).expanduser().read_text(encoding="utf-8", errors="replace")
        raw = re.sub(r"^WEBVTT.*?\n", "", raw, flags=re.I | re.M)
        blocks = re.split(r"\n\s*\n", raw.strip())
        subs: list[dict[str, Any]] = []
        for block in blocks:
            lines = [ln.rstrip() for ln in block.splitlines()]
            lines = [ln for ln in lines if ln and not ln.startswith("NOTE")]
            if len(lines) < 2:
                continue
            time_line = lines[0]
            if "-->" not in time_line:
                continue
            left, right = time_line.split("-->", 1)
            st = _parse_timestamp(left.replace(".", ","))
            en = _parse_timestamp(right.split()[0].replace(".", ","))
            text = "\n".join(lines[1:])
            text = re.sub(r"<[^>]+>", "", text)
            subs.append(
                {
                    "id": f"sub_{uuid.uuid4().hex[:10]}",
                    "start": round(st, 4),
                    "end": round(en, 4),
                    "text": text,
                    "style": {
                        "font_size": 42,
                        "color": "white",
                        "outline_color": "black",
                        "outline_width": 2,
                        "position": "bottom",
                    },
                }
            )
        project["subtitles"] = subs
        return project

    def add_subtitle(
        self,
        project: dict[str, Any],
        start: float,
        end: float,
        text: str,
        *,
        style: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        subs = project.setdefault("subtitles", [])
        st = style or {
            "font_size": 42,
            "color": "white",
            "outline_color": "black",
            "outline_width": 2,
            "position": "bottom",
        }
        subs.append(
            {
                "id": f"sub_{uuid.uuid4().hex[:10]}",
                "start": float(start),
                "end": float(end),
                "text": str(text),
                "style": dict(st),
            }
        )
        return project

    def export_ass(self, project: dict[str, Any], output_ass_path: str) -> str:
        out = Path(output_ass_path).expanduser()
        out.parent.mkdir(parents=True, exist_ok=True)
        w = int(project.get("width") or 1080)
        h = int(project.get("height") or 1920)
        lines: list[str] = [
            "[Script Info]",
            "ScriptType: v4.00+",
            f"PlayResX: {w}",
            f"PlayResY: {h}",
            "WrapStyle: 0",
            "",
            "[V4+ Styles]",
            "Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding",
            "Style: Default,Arial,42,&H00FFFFFF,&H000000FF,&H00000000,&H80000000,0,0,0,0,100,100,0,0,1,2,2,2,20,20,40,1",
            "",
            "[Events]",
            "Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text",
        ]

        def fmt_ass_time(sec: float) -> str:
            h = int(sec // 3600)
            m = int((sec % 3600) // 60)
            s = sec % 60
            cs = int(round((s - int(s)) * 100))
            s = int(s)
            return f"{h}:{m:02d}:{s:02d}.{cs:02d}"

        for sub in sorted(project.get("subtitles") or [], key=lambda x: float(x.get("start") or 0)):
            if not isinstance(sub, dict):
                continue
            st = float(sub.get("start") or 0)
            en = float(sub.get("end") or 0)
            txt = _ass_escape(str(sub.get("text") or "").replace("\n", "\\N"))
            lines.append(f"Dialogue: 0,{fmt_ass_time(st)},{fmt_ass_time(en)},Default,,0,0,0,,{txt}")

        body = "\n".join(lines) + "\n"
        out.write_text(body, encoding="utf-8-sig")
        return str(out.resolve())
