"""Transition giữa hai clip — metadata trong project; FFmpeg xfade trong builder."""

from __future__ import annotations

import uuid
from typing import Any


class TransitionManager:
    XFADE_NAMES = {
        "crossfade": "fade",
        "fade_black": "fade",
        "slide_left": "slideleft",
        "slide_right": "slideright",
        "zoom_in": "zoomin",
        "wipe": "wiperight",
    }

    def add_transition(
        self,
        project: dict[str, Any],
        from_clip_id: str,
        to_clip_id: str,
        transition_type: str,
        duration: float,
        *,
        start_time: float | None = None,
    ) -> dict[str, Any]:
        transitions = project.setdefault("transitions", [])
        tid = f"transition_{uuid.uuid4().hex[:10]}"
        st = start_time
        if st is None:
            st = self._default_start_time(project, from_clip_id, float(duration))
        transitions.append(
            {
                "id": tid,
                "type": str(transition_type),
                "from_clip_id": str(from_clip_id),
                "to_clip_id": str(to_clip_id),
                "start_time": float(st),
                "duration": float(duration),
            }
        )
        return project

    def _default_start_time(self, project: dict[str, Any], from_clip_id: str, dur: float) -> float:
        video_tr = None
        for tr in project.get("tracks") or []:
            if isinstance(tr, dict) and str(tr.get("type")) == "video":
                video_tr = tr
                break
        if not video_tr:
            return 0.0
        for cl in video_tr.get("clips") or []:
            if isinstance(cl, dict) and str(cl.get("id")) == str(from_clip_id):
                ts = float(cl.get("timeline_start") or 0)
                du = float(cl.get("duration") or 0)
                return max(0.0, ts + du - dur)
        return 0.0

    def remove_transition(self, project: dict[str, Any], transition_id: str) -> dict[str, Any]:
        tid = str(transition_id)
        project["transitions"] = [
            x for x in (project.get("transitions") or []) if not (isinstance(x, dict) and str(x.get("id")) == tid)
        ]
        return project
