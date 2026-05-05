"""Dựng lệnh FFmpeg từ project JSON — MVP + Phase 2 (tùy chọn)."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from src.services.video_editor.audio_filter_builder import AudioFilterBuilder
from src.services.video_editor.audio_mix_manager import AudioMixManager
from src.services.video_editor.canvas_filter_builder import CanvasFilterBuilder
from src.services.video_editor.keyframe_animation_manager import KeyframeAnimationManager
from src.services.video_editor.speed_manager import SpeedManager
from src.services.video_editor.transition_manager import TransitionManager
from src.services.video_editor.video_filter_manager import VideoFilterManager
from src.services.video_editor.random_motion_expr import drawtext_random_xy_expr, overlay_random_xy_expr
from src.services.video_editor.video_transform_filter_builder import VideoTransformFilterBuilder, ensure_video_transform_defaults


def _escape_drawtext(s: str) -> str:
    return (
        str(s)
        .replace("\\", r"\\")
        .replace("'", r"\'")
        .replace(":", r"\:")
        .replace("%", r"\%")
    )


def _norm_os_path(p: Path) -> str:
    return os.path.normpath(str(p.resolve()))


def _ass_path_filter(path: Path) -> str:
    """Escape đường dẫn Windows cho filter ass=subtitles=."""
    s = path.resolve().as_posix()
    return s.replace("\\", "/").replace(":", "\\:")


class FFmpegCommandBuilder:
    """
    MVP: trim + concat, overlay, text, volume/fade.
    Phase 2: speed, filter màu, transition xfade, subtitle ASS, BGM + ducking, overlay animation.
    """

    def __init__(self) -> None:
        self._vf = VideoFilterManager()
        self._km = KeyframeAnimationManager()
        self._tm = TransitionManager()
        self._vtf = VideoTransformFilterBuilder()
        self._cvb = CanvasFilterBuilder()
        self._afb = AudioFilterBuilder()

    def build_export_command(
        self,
        project: dict[str, Any],
        output_path: str,
        *,
        ffmpeg_bin: str,
        ass_path: str | None = None,
        output_duration_limit_sec: float | None = None,
        encoding_overrides: dict[str, Any] | None = None,
    ) -> list[str]:
        ff = _norm_os_path(Path(ffmpeg_bin))
        out = _norm_os_path(Path(output_path).expanduser())

        features = project.get("features") or {}
        w = int(project.get("width") or 1080)
        h = int(project.get("height") or 1920)
        fps = float(project.get("fps") or 30)

        exp = project.get("export") or {}
        enc = encoding_overrides or {}
        vcodec = str(enc.get("codec") if enc.get("codec") is not None else exp.get("codec") or "libx264")
        preset = str(enc.get("preset") if enc.get("preset") is not None else exp.get("preset") or "veryfast")
        crf = int(enc.get("crf") if enc.get("crf") is not None else exp.get("crf") if exp.get("crf") is not None else 23)
        acodec = str(enc.get("audio_codec") if enc.get("audio_codec") is not None else exp.get("audio_codec") or "aac")

        media_by_id = {str(m.get("id")): m for m in (project.get("media") or []) if isinstance(m, dict) and m.get("id")}

        def resolve_path(media: dict[str, Any]) -> Path | None:
            lp = str(media.get("local_path") or "").strip()
            op = str(media.get("path") or "").strip()
            for candidate in (lp, op):
                if not candidate:
                    continue
                p = Path(candidate).expanduser()
                if p.is_file():
                    return p.resolve()
            return None

        tracks = project.get("tracks") or []
        video_clips: list[dict[str, Any]] = []
        overlay_clips: list[dict[str, Any]] = []
        text_clips: list[dict[str, Any]] = []
        audio_timeline_clips: list[dict[str, Any]] = []
        for tr in tracks:
            if not isinstance(tr, dict):
                continue
            tt = str(tr.get("type") or "")
            if tt == "video":
                for cl in tr.get("clips") or []:
                    if isinstance(cl, dict) and str(cl.get("type") or "") == "video":
                        video_clips.append(cl)
            elif tt == "overlay":
                for cl in tr.get("clips") or []:
                    if isinstance(cl, dict):
                        overlay_clips.append(cl)
            elif tt == "text":
                for cl in tr.get("clips") or []:
                    if isinstance(cl, dict):
                        text_clips.append(cl)
            elif tt == "audio":
                for cl in tr.get("clips") or []:
                    if isinstance(cl, dict) and str(cl.get("type") or "") == "audio":
                        audio_timeline_clips.append(cl)

        video_clips.sort(key=lambda c: float(c.get("timeline_start") or 0))
        overlay_clips.sort(key=lambda c: float(c.get("timeline_start") or 0))
        audio_timeline_clips.sort(key=lambda c: float(c.get("timeline_start") or 0))

        inputs: list[tuple[str, Any, dict[str, Any]]] = []

        def file_input_index(path: Path, extra: dict[str, Any] | None = None) -> int:
            meta = dict(extra or {})
            sl = meta.get("stream_loop")
            rp = str(path.resolve())
            for i, (k, v, m) in enumerate(inputs):
                if k != "file":
                    continue
                if str(v.resolve()) != rp:
                    continue
                if (m or {}).get("stream_loop") == sl:
                    return i
            idx = len(inputs)
            inputs.append(("file", path, meta))
            return idx

        def silence_input_index(duration: float) -> int:
            idx = len(inputs)
            inputs.append(("silence", float(duration), {}))
            return idx

        def clip_speed(cl: dict[str, Any]) -> float:
            try:
                s = float(cl.get("speed") or 1.0)
                return s if s > 0 else 1.0
            except (TypeError, ValueError):
                return 1.0

        def color_vf_for(clip_id: str) -> str:
            if not features.get("color_filters", True):
                return ""
            for f in project.get("filters") or []:
                if isinstance(f, dict) and str(f.get("clip_id")) == str(clip_id):
                    return self._vf.build_ffmpeg_filter(f).strip()
            return ""

        fc: list[str] = []
        seg_v_labels: list[str] = []
        seg_a_labels: list[str] = []
        seg_durations: list[float] = []

        sm = SpeedManager()

        for si, clip in enumerate(video_clips):
            mid = str(clip.get("media_id") or "")
            media = media_by_id.get(mid)
            if not media:
                raise ValueError(f"Thiếu media {mid}")
            vp = resolve_path(media)
            if not vp:
                raise ValueError(f"Không resolve được file media {mid}")
            ensure_video_transform_defaults(clip, project)
            vi = file_input_index(vp)
            ss = float(clip.get("source_start") or 0)
            se = float(clip.get("source_end") or 0)
            du = float(clip.get("duration") or 0)
            sp = clip_speed(clip)
            fi = float(clip.get("fade_in") or 0)
            fo = float(clip.get("fade_out") or 0)

            has_audio = bool(media.get("has_audio", True))

            vlab = f"sv{si}"
            alab = f"sa{si}"
            pre_lab = f"pre{si}"
            cv_mid = f"cv{si}"

            vf_speed, af_speed = sm.build_speed_filter(sp)
            col_vf = color_vf_for(str(clip.get("id") or ""))
            tf = self._vtf.build_transform_filters(clip, project).strip()
            vol_fade = self._afb.build_volume_fade_filters(clip, du)

            vchain = f"[{vi}:v]trim=start={ss}:end={se},setpts=PTS-STARTPTS"
            if vf_speed:
                vchain += f",{vf_speed}"
            if tf:
                vchain += f",{tf}"
            vchain += f"[{pre_lab}]"
            fc.append(vchain)

            bb = clip.get("blur_background") or {}
            if isinstance(bb, dict) and bb.get("enabled"):
                blur_lines = self._cvb.build_blur_background_chain(
                    pre_lab,
                    cv_mid,
                    w,
                    h,
                    int(bb.get("blur") or 20),
                    seg_index=si,
                )
                fc.extend(blur_lines)
            else:
                cv_vf = self._cvb.build_simple_canvas_vf(clip, w, h)
                fc.append(f"[{pre_lab}]{cv_vf}[{cv_mid}]")

            v_up = f"[{cv_mid}]fps={fps}"
            if col_vf:
                v_up += f",{col_vf}"
            if fi > 0:
                v_up += f",fade=t=in:st=0:d={fi}"
            if fo > 0 and du > fo:
                st_out = max(0.0, du - fo)
                v_up += f",fade=t=out:st={st_out}:d={fo}"
            v_up += f"[{vlab}]"
            fc.append(v_up)

            if has_audio:
                achain = f"[{vi}:a]atrim=start={ss}:end={se},asetpts=PTS-STARTPTS"
                if af_speed:
                    achain += f",{af_speed}"
                achain += ",aresample=48000"
                if vol_fade:
                    achain += f",{vol_fade}"
                achain += f"[{alab}]"
                fc.append(achain)
            else:
                ai = silence_input_index(du)
                achain = f"[{ai}:a]atrim=0:{du},asetpts=PTS-STARTPTS,aresample=48000"
                if vol_fade:
                    achain += f",{vol_fade}"
                achain += f"[{alab}]"
                fc.append(achain)

            seg_v_labels.append(vlab)
            seg_a_labels.append(alab)
            seg_durations.append(du)

        if not seg_v_labels:
            raise ValueError("Không có đoạn video.")

        trans_by_pair: dict[tuple[str, str], dict[str, Any]] = {}
        for t in project.get("transitions") or []:
            if isinstance(t, dict) and t.get("from_clip_id") and t.get("to_clip_id"):
                trans_by_pair[(str(t["from_clip_id"]), str(t["to_clip_id"]))] = t

        use_xfade = bool(features.get("transitions", True)) and len(seg_v_labels) >= 2
        if use_xfade:
            for i in range(len(video_clips) - 1):
                a, b = video_clips[i], video_clips[i + 1]
                if (str(a.get("id")), str(b.get("id"))) not in trans_by_pair:
                    use_xfade = False
                    break

        if use_xfade:
            cur_v = seg_v_labels[0]
            cur_a = seg_a_labels[0]
            cum_d = seg_durations[0]
            for i in range(len(seg_v_labels) - 1):
                tr = trans_by_pair[(str(video_clips[i]["id"]), str(video_clips[i + 1]["id"]))]
                ttyp = str(tr.get("type") or "crossfade")
                T = float(tr.get("duration") or 0.5)
                T = max(0.05, min(T, seg_durations[i], seg_durations[i + 1]) - 1e-3)
                xname = self._tm.XFADE_NAMES.get(ttyp, "fade")
                off = max(0.0, cum_d - T)
                nv = f"xv{i}"
                na = f"xa{i}"
                fc.append(
                    f"[{cur_v}][{seg_v_labels[i + 1]}]xfade=transition={xname}:duration={T}:offset={off}[{nv}]"
                )
                fc.append(
                    f"[{cur_a}][{seg_a_labels[i + 1]}]acrossfade=d={T}:c1=tri:c2=tri[{na}]"
                )
                cur_v, cur_a = nv, na
                cum_d = cum_d + seg_durations[i + 1] - T
            fc.append(f"[{cur_v}]format=yuv420p[basev]")
            fc.append(f"[{cur_a}]aresample=48000[basea]")
            current_v = "basev"
        else:
            # concat=n:v=1:a=1 bắt buộc thứ tự [v0][a0][v1][a1]… — không được gom hết v rồi hết a.
            concat_in = "".join(f"[{seg_v_labels[i]}][{seg_a_labels[i]}]" for i in range(len(seg_v_labels)))
            fc.append(f"{concat_in}concat=n={len(seg_v_labels)}:v=1:a=1[basev][basea]")
            current_v = "basev"

        for oi, ovc in enumerate(overlay_clips):
            mid = str(ovc.get("media_id") or "")
            media = media_by_id.get(mid)
            if not media:
                continue
            ip = resolve_path(media)
            if not ip:
                continue
            # Ảnh tĩnh: lặp stream (-stream_loop -1) để overlay đủ suốt timeline; không thì FFmpeg chỉ có 1 frame.
            mtype = str(media.get("type") or "")
            in_meta: dict[str, Any] = {}
            if mtype == "image":
                in_meta["stream_loop"] = -1
            ii = file_input_index(ip, in_meta or None)
            ow = int(ovc.get("width") or 180)
            oh = int(ovc.get("height") or 180)
            if ow < 2:
                ow = 2
            if oh < 2:
                oh = 2
            ts = float(ovc.get("timeline_start") or 0)
            od = float(ovc.get("duration") or 0)
            te = ts + od
            olab = f"ov{oi}"
            out_lab = f"ovout{oi}"

            anim = self._km.build_overlay_expression(ovc) if features.get("animation", True) else {"use_expr": False, "x": int(ovc.get("x") or 0), "y": int(ovc.get("y") or 0), "enable": f"between(t,{ts},{te})"}
            ox, oy = int(ovc.get("x") or 0), int(ovc.get("y") or 0)
            extra_vf = str(anim.get("extra_vf") or "")
            try:
                opa = float(ovc.get("opacity") if ovc.get("opacity") is not None else 1.0)
            except (TypeError, ValueError):
                opa = 1.0
            opa = max(0.0, min(1.0, opa))

            scale_chain = f"[{ii}:v]scale={ow}:{oh}"
            if extra_vf:
                scale_chain += f",{extra_vf}"
            if opa < 0.999:
                scale_chain += f",format=rgba,colorchannelmixer=aa={opa:.5f}"
            scale_chain += f"[{olab}]"

            rand_m = bool(ovc.get("random_motion_enabled"))
            try:
                r_int = float(ovc.get("random_motion_interval") or 2.0)
            except (TypeError, ValueError):
                r_int = 2.0
            try:
                r_seed = int(ovc.get("random_motion_seed") or 0)
            except (TypeError, ValueError):
                r_seed = 0
            r_smooth = bool(ovc.get("random_motion_smooth", False))

            if rand_m:
                xex, yex = overlay_random_xy_expr(r_int, seed=r_seed, smooth=r_smooth)
                fc.append(
                    f"{scale_chain};"
                    f"[{current_v}][{olab}]overlay=x={xex}:y={yex}:enable='between(t,{ts},{te})'[{out_lab}]"
                )
            elif anim.get("use_expr"):
                xex = str(anim.get("x_expr") or ox)
                yex = str(anim.get("y_expr") or oy)
                fc.append(
                    f"{scale_chain};"
                    f"[{current_v}][{olab}]overlay=x={xex}:y={yex}:enable='between(t,{ts},{te})'[{out_lab}]"
                )
            else:
                fc.append(
                    f"{scale_chain};"
                    f"[{current_v}][{olab}]overlay={ox}:{oy}:enable='between(t,{ts},{te})'[{out_lab}]"
                )
            current_v = out_lab

        default_fontfile = _escape_drawtext("C:/Windows/Fonts/arial.ttf") if os.name == "nt" else ""

        for ti, tc in enumerate(text_clips):
            txt = str(tc.get("text") or "")
            if not txt.strip():
                continue
            ts = float(tc.get("timeline_start") or 0)
            td = float(tc.get("duration") or 0)
            te = ts + td
            tx = int(tc.get("x") or 0)
            ty = int(tc.get("y") or 0)
            fs = int(tc.get("font_size") or 48)
            col = str(tc.get("color") or "white")
            ff_path = str(tc.get("font_file") or "").strip()
            esc = _escape_drawtext(txt)
            fontpart = ""
            if ff_path:
                fp_esc = _escape_drawtext(_norm_os_path(Path(ff_path)))
                fontpart = f":fontfile='{fp_esc}'"
            elif default_fontfile:
                fontpart = f":fontfile='{default_fontfile}'"

            rand_txt = bool(tc.get("random_motion_enabled"))
            try:
                r_txt = float(tc.get("random_motion_interval") or 2.0)
            except (TypeError, ValueError):
                r_txt = 2.0
            try:
                r_seed_t = int(tc.get("random_motion_seed") or 0)
            except (TypeError, ValueError):
                r_seed_t = 0
            r_smooth_t = bool(tc.get("random_motion_smooth", False))

            tlab = f"tv{ti}"
            if rand_txt:
                xex_t, yex_t = drawtext_random_xy_expr(r_txt, seed=r_seed_t, smooth=r_smooth_t)
                fc.append(
                    f"[{current_v}]drawtext=text='{esc}'{fontpart}:x={xex_t}:y={yex_t}:fontsize={fs}"
                    f":fontcolor={col}:enable='between(t,{ts},{te})'[{tlab}]"
                )
            else:
                fc.append(
                    f"[{current_v}]drawtext=text='{esc}'{fontpart}:x={tx}:y={ty}:fontsize={fs}"
                    f":fontcolor={col}:enable='between(t,{ts},{te})'[{tlab}]"
                )
            current_v = tlab

        final_v = current_v

        ass_file = ass_path

        if ass_file and Path(ass_file).is_file():
            ap = Path(ass_file)
            fc.append(f"[{final_v}]ass={_ass_path_filter(ap)}[vsub]")
            final_v = "vsub"
        final_audio = "basea"
        if str(project.get("audio_mode") or "mix").lower() == "replace":
            fc.append(f"[basea]volume=0[basea_z]")
            final_audio = "basea_z"
        au = project.get("audio_settings") or {}
        bgm_list = au.get("bgm") or []
        if features.get("bgm", True) and bgm_list:
            amix = AudioMixManager()
            duck = au.get("ducking") or []
            for bi, bg in enumerate(bgm_list):
                if not isinstance(bg, dict):
                    continue
                bmid = str(bg.get("media_id") or "")
                bm = media_by_id.get(bmid)
                if not bm:
                    continue
                bp = resolve_path(bm)
                if not bp:
                    continue
                loop_meta = {"stream_loop": -1} if bg.get("loop") else {}
                bidx = file_input_index(bp, loop_meta)
                vol = float(bg.get("volume") if bg.get("volume") is not None else 0.25)
                t0_bg = float(bg.get("timeline_start") or 0)
                du_b = float(bg.get("duration") or float(project.get("duration") or 60))
                proj_dur_bgm = float(project.get("duration") or 0)
                if proj_dur_bgm > 0 and bool(bg.get("loop", True)):
                    du_b = max(du_b, max(0.0, proj_dur_bgm - t0_bg))
                fi_b = float(bg.get("fade_in") or 0)
                fo_b = float(bg.get("fade_out") or 0)
                expr = amix.build_bgm_volume_expression(vol, duck if features.get("ducking", True) else [])
                blab = f"bgm{bi}"
                chain = f"[{bidx}:a]atrim=0:{du_b},asetpts=PTS-STARTPTS,aresample=48000"
                if fi_b > 0:
                    chain += f",afade=t=in:st=0:d={fi_b}"
                if fo_b > 0 and du_b > fo_b:
                    chain += f",afade=t=out:st={max(0.0, du_b - fo_b)}:d={fo_b}"
                if t0_bg > 0:
                    dm = max(0, int(round(t0_bg * 1000)))
                    chain += f",adelay={dm}|{dm}"
                chain += f",volume='{expr}':eval=frame[{blab}]"
                fc.append(chain)
                mix_out = f"aout{bi}"
                fc.append(f"[{final_audio}][{blab}]amix=inputs=2:duration=first:dropout_transition=2[{mix_out}]")
                final_audio = mix_out

        if features.get("timeline_audio", True) and audio_timeline_clips:
            for ai, acl in enumerate(audio_timeline_clips):
                mid_a = str(acl.get("media_id") or "")
                amedia = media_by_id.get(mid_a)
                if not amedia:
                    continue
                if str(amedia.get("type") or "") != "audio":
                    continue
                apath = resolve_path(amedia)
                if not apath:
                    continue
                aidx = file_input_index(apath)
                ss_a = float(acl.get("source_start") or 0)
                se_a = float(acl.get("source_end") or 0)
                du_a = float(acl.get("duration") or 0)
                if se_a <= ss_a and du_a > 0:
                    se_a = ss_a + du_a
                elif se_a <= ss_a:
                    se_a = ss_a + 0.1
                ts_a = float(acl.get("timeline_start") or 0)
                src_len = max(1e-3, float(se_a) - float(ss_a))
                proj_dur_tl = float(project.get("duration") or 0)
                out_dur = float(du_a)
                if proj_dur_tl > 0:
                    out_dur = max(out_dur, max(0.0, proj_dur_tl - ts_a))
                use_aloop = bool(acl.get("loop", True)) and out_dur > src_len + 0.02
                vol_fade_a = self._afb.build_volume_fade_filters(acl, out_dur if use_aloop else du_a)
                delay_ms_a = max(0, int(round(ts_a * 1000)))
                alab_tl = f"tlau{ai}"
                chain_tl = f"[{aidx}:a]atrim=start={ss_a}:end={se_a},asetpts=PTS-STARTPTS,aresample=48000"
                if use_aloop:
                    chain_tl += f",aloop=loop=-1:size=0,atrim=0:{out_dur:.6f},asetpts=PTS-STARTPTS"
                if vol_fade_a:
                    chain_tl += f",{vol_fade_a}"
                if delay_ms_a > 0:
                    chain_tl += f",adelay={delay_ms_a}|{delay_ms_a}"
                chain_tl += f"[{alab_tl}]"
                fc.append(chain_tl)
                mix_tl = f"tlmix{ai}"
                fc.append(
                    f"[{final_audio}][{alab_tl}]amix=inputs=2:duration=first:dropout_transition=2[{mix_tl}]"
                )
                final_audio = mix_tl

        args: list[str] = [ff, "-y"]
        for typ, val, meta in inputs:
            if typ == "file":
                if meta.get("stream_loop") is not None:
                    args.extend(["-stream_loop", str(meta["stream_loop"])])
                args.extend(["-i", _norm_os_path(val)])
            else:
                d = float(val)
                args.extend(
                    [
                        "-f",
                        "lavfi",
                        "-t",
                        f"{d:.6f}",
                        "-i",
                        "anullsrc=channel_layout=stereo:sample_rate=48000",
                    ]
                )

        fc_str = ";".join(fc)
        args.extend(
            [
                "-filter_complex",
                fc_str,
                "-map",
                f"[{final_v}]",
                "-map",
                f"[{final_audio}]",
                "-c:v",
                vcodec,
                "-preset",
                preset,
                "-crf",
                str(crf),
                "-c:a",
                acodec,
                "-movflags",
                "+faststart",
            ]
        )
        if output_duration_limit_sec is not None and float(output_duration_limit_sec) > 0:
            args.extend(["-t", f"{float(output_duration_limit_sec):.4f}"])
        args.append(out)

        return args
