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
from src.services.video_editor.timeline_manager import compute_video_timeline_end
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
    """Chuẩn hoá đường dẫn cho FFmpeg; Windows: bơm \\\\?\\ khi dài để tránh MAX_PATH."""
    try:
        resolved = p.expanduser().resolve(strict=False)
    except (OSError, RuntimeError):
        resolved = p.expanduser()
    s = os.path.normpath(str(resolved))
    if os.name != "nt":
        return s
    if str(os.environ.get("TOOLFB_FFMPEG_NO_LONGPATH", "0") or "0").strip().lower() in ("1", "true", "yes", "on"):
        return s
    if s.startswith("\\\\?\\"):
        return s
    if len(s) <= 220:
        return s
    if len(s) >= 2 and s[1] == ":":
        return "\\\\?\\" + s
    if s.startswith("\\\\"):
        return "\\\\?\\UNC\\" + s[2:]
    return s


def _ass_path_filter(path: Path) -> str:
    """Escape đường dẫn Windows cho filter ass=subtitles=."""
    s = path.resolve().as_posix()
    return s.replace("\\", "/").replace(":", "\\:")


def _normalize_video_source_trim(ss: float, se: float, du: float, speed: float) -> tuple[float, float]:
    """
    Sau trim, setpts=PTS/speed làm độ dài output ≈ (source_end-source_start)/speed.
    Ta cần source_end-source_start ≈ duration_timeline × speed.
    Nếu source_end quá xa (metadata lệch / source_end = cả file), FFmpeg vẫn giải mã hàng phút
    dù timeline chỉ vài giây — thu hẹp theo duration.
    """
    try:
        sp = float(speed)
    except (TypeError, ValueError):
        sp = 1.0
    if sp <= 0:
        sp = 1.0
    du = max(0.0, float(du))
    ss = max(0.0, float(ss))
    se = float(se)
    need = max(0.05, du * sp)
    tol = 0.08
    if se <= ss + 1e-6:
        return ss, ss + need
    if (se - ss) > need + tol:
        return ss, ss + need
    return ss, se


def _overlay_filter_params(*, ts: float, te: float, x: str, y: str) -> str:
    """Ghép logo RGBA lên video YUV — format=auto + lặp frame khi ảnh tĩnh chỉ có 1 frame."""
    return f"{x}:{y}:format=auto:eof_action=repeat:enable='between(t,{ts},{te})'"


def _build_still_image_overlay_chain(
    input_idx: int,
    *,
    ow: int,
    oh: int,
    fps: float,
    opacity: float,
    extra_vf: str,
    out_label: str,
) -> str:
    """
    Chuỗi filter cho logo/ảnh tĩnh (PNG trong suốt).

    - pad nền trong suốt, giữ alpha khi scale
    - loop size=1 (ảnh 1 frame) — size=32767 dễ làm mất/loạn alpha trên PNG
    - fps khớp timeline để overlay không «chỉ hiện frame đầu»
    """
    ifr = max(1, min(120, int(round(float(fps)))))
    ow = max(2, int(ow))
    oh = max(2, int(oh))
    opa = max(0.0, min(1.0, float(opacity)))
    parts: list[str] = [
        f"[{input_idx}:v]scale={ow}:{oh}:force_original_aspect_ratio=decrease:flags=lanczos",
        f"pad={ow}:{oh}:(ow-iw)/2:(oh-ih)/2:color=0x00000000",
        "format=rgba",
        "loop=loop=-1:size=1:start=0",
        f"fps={ifr}",
        f"setpts=N/{ifr}/TB",
    ]
    ev = str(extra_vf or "").strip().lstrip(",")
    if ev:
        parts.append(ev)
    if opa < 0.999:
        parts.append(f"colorchannelmixer=aa={opa:.6f}")
    parts.append("format=rgba")
    parts.append(f"[{out_label}]")
    return ",".join(parts)


def _build_video_overlay_chain(
    input_idx: int,
    *,
    ow: int,
    oh: int,
    fps: float,
    opacity: float,
    extra_vf: str,
    out_label: str,
    loop_size: int | None = None,
) -> str:
    """Clip overlay dạng video/GIF — vẫn ép RGBA + fps để alpha ổn định."""
    ifr = max(1, min(120, int(round(float(fps)))))
    ow = max(2, int(ow))
    oh = max(2, int(oh))
    opa = max(0.0, min(1.0, float(opacity)))
    parts: list[str] = [
        f"[{input_idx}:v]scale={ow}:{oh}:flags=lanczos",
        "format=rgba",
    ]
    if loop_size is not None:
        ls = max(0, int(loop_size))
        parts.append(f"loop=loop=-1:size={ls}:start=0")
    parts.append(f"fps={ifr}")
    ev = str(extra_vf or "").strip().lstrip(",")
    if ev:
        parts.append(ev)
    if opa < 0.999:
        parts.append(f"colorchannelmixer=aa={opa:.6f}")
    parts.append("format=rgba")
    parts.append(f"[{out_label}]")
    return ",".join(parts)


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
        lightweight_mode_override: bool | None = None,
        mp4_faststart: bool | None = None,
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
        enc_threads = enc.get("threads")
        # Mặc định ưu tiên "nhẹ + nhanh" (TOOLFB_LIGHT_EXPORT mặc định bật).
        # Tune thêm:
        # - TOOLFB_EXPORT_MIN_CRF, TOOLFB_EXPORT_LIGHT_PRESET (vd veryfast)
        # - TOOLFB_EXPORT_AUDIO_BITRATE
        # - TOOLFB_EXPORT_VBV=1 hoặc TOOLFB_EXPORT_MAXRATE_K để kẹp bitrate (chậm hơn, file gọn hơn)
        # - TOOLFB_EXPORT_THREADS (để trống = tự chọn theo CPU, tối đa 12)
        if lightweight_mode_override is None:
            lightweight_mode = str(os.environ.get("TOOLFB_LIGHT_EXPORT", "1") or "").strip().lower() not in (
                "0",
                "false",
                "off",
                "no",
            )
        else:
            lightweight_mode = bool(lightweight_mode_override)
        if lightweight_mode:
            try:
                min_crf = int(float(str(os.environ.get("TOOLFB_EXPORT_MIN_CRF", "28") or "28")))
            except (TypeError, ValueError):
                min_crf = 28
            crf = max(crf, max(18, min_crf))
            # Giới hạn tốc độ encode: không cho preset «chậm» hơn mức target (mặc định veryfast).
            # Trước đây chỉ «kéo chậm» từ ultrafast → fast, nên project preset medium/slow vẫn export rất lâu.
            preset_order = [
                "ultrafast",
                "superfast",
                "veryfast",
                "faster",
                "fast",
                "medium",
                "slow",
                "slower",
                "veryslow",
            ]
            p = preset.strip().lower()
            target_preset = str(os.environ.get("TOOLFB_EXPORT_LIGHT_PRESET", "veryfast") or "veryfast").strip().lower()
            if target_preset not in preset_order:
                target_preset = "veryfast"
            if p in preset_order:
                pi = preset_order.index(p)
                ti = preset_order.index(target_preset)
                # Chỉ số nhỏ hơn = encode nhanh hơn. Cần preset nhanh hơn hoặc bằng target → lấy min(pi, ti).
                preset = preset_order[min(pi, ti)]

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

        def color_vf_for(clip_dict: dict[str, Any]) -> str:
            if not features.get("color_filters", True):
                return ""
            return self._vf.build_clip_color_adjust_vf(clip_dict, project).strip()

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
            ss, se = _normalize_video_source_trim(ss, se, du, sp)
            fi = float(clip.get("fade_in") or 0)
            fo = float(clip.get("fade_out") or 0)

            has_audio = bool(media.get("has_audio", True))

            vlab = f"sv{si}"
            alab = f"sa{si}"
            pre_lab = f"pre{si}"
            cv_mid = f"cv{si}"

            vf_speed, af_speed = sm.build_speed_filter(sp)
            col_vf = color_vf_for(clip)
            tf = self._vtf.build_transform_filters(clip, project).strip()
            vol_fade = self._afb.build_volume_fade_filters(clip, du)

            vchain = f"[{vi}:v]trim=start={ss}:end={se},setpts=PTS-STARTPTS"
            if vf_speed:
                vchain += f",{vf_speed}"
            if tf:
                vchain += f",{tf}"
            vchain += f"[{pre_lab}]"
            fc.append(vchain)

            cv_vf = self._cvb.build_simple_canvas_vf(clip, w, h)
            zoom_vf = self._cvb.build_canvas_zoom_vf(clip, w, h).strip()
            if zoom_vf:
                fc.append(f"[{pre_lab}]{cv_vf},{zoom_vf}[{cv_mid}]")
            else:
                fc.append(f"[{pre_lab}]{cv_vf}[{cv_mid}]")

            # concat / xfade bắt buộc khớp width, height, SAR và pixel format giữa mọi đoạn.
            # Một số nguồn (đặc biệt vertical phone) có SAR lạ; scale+pad trước đó đôi khi vẫn để SAR khác 1:1
            # hoặc kích thước lệch nhẹ → concat báo lỗi (Invalid argument / configure concat).
            v_up = f"[{cv_mid}]fps={fps}"
            if col_vf:
                v_up += f",{col_vf}"
            if fi > 0:
                v_up += f",fade=t=in:st=0:d={fi}"
            if fo > 0 and du > fo:
                st_out = max(0.0, du - fo)
                v_up += f",fade=t=out:st={st_out}:d={fo}"
            v_up += (
                f",scale={w}:{h}:force_original_aspect_ratio=decrease,"
                f"pad={w}:{h}:(ow-iw)/2:(oh-ih)/2:black,setsar=1,format=yuv420p"
            )
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
            master_av_dur = float(cum_d)
        else:
            # concat=n:v=1:a=1 bắt buộc thứ tự [v0][a0][v1][a1]… — không được gom hết v rồi hết a.
            concat_in = "".join(f"[{seg_v_labels[i]}][{seg_a_labels[i]}]" for i in range(len(seg_v_labels)))
            fc.append(f"{concat_in}concat=n={len(seg_v_labels)}:v=1:a=1[basev][basea]")
            current_v = "basev"
            master_av_dur = float(sum(seg_durations))

        for oi, ovc in enumerate(overlay_clips):
            mid = str(ovc.get("media_id") or "")
            media = media_by_id.get(mid)
            if not media:
                continue
            ip = resolve_path(media)
            if not ip:
                continue
            # Ảnh tĩnh: KHÔNG dùng -stream_loop -1 (decode PNG lặp vô hạn — dễ lỗi zlib / crash 0xC0000005 sau encode dài).
            # Giải mã một lần rồi loop trong filter_complex (xem scale_chain bên dưới).
            mtype = str(media.get("type") or "")
            in_meta: dict[str, Any] = {}
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

            is_still = mtype == "image"
            if is_still and ip.suffix.lower() == ".gif":
                scale_chain = _build_video_overlay_chain(
                    ii,
                    ow=ow,
                    oh=oh,
                    fps=float(fps),
                    opacity=opa,
                    extra_vf=extra_vf,
                    out_label=olab,
                    loop_size=0,
                )
            elif is_still:
                scale_chain = _build_still_image_overlay_chain(
                    ii,
                    ow=ow,
                    oh=oh,
                    fps=float(fps),
                    opacity=opa,
                    extra_vf=extra_vf,
                    out_label=olab,
                )
            else:
                scale_chain = _build_video_overlay_chain(
                    ii,
                    ow=ow,
                    oh=oh,
                    fps=float(fps),
                    opacity=opa,
                    extra_vf=extra_vf,
                    out_label=olab,
                )

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
                ov_p = _overlay_filter_params(ts=ts, te=te, x=str(xex), y=str(yex))
                fc.append(f"{scale_chain};[{current_v}][{olab}]overlay={ov_p}[{out_lab}]")
            elif anim.get("use_expr"):
                xex = str(anim.get("x_expr") or ox)
                yex = str(anim.get("y_expr") or oy)
                ov_p = _overlay_filter_params(ts=ts, te=te, x=xex, y=yex)
                fc.append(f"{scale_chain};[{current_v}][{olab}]overlay={ov_p}[{out_lab}]")
            else:
                ov_p = _overlay_filter_params(ts=ts, te=te, x=str(ox), y=str(oy))
                fc.append(f"{scale_chain};[{current_v}][{olab}]overlay={ov_p}[{out_lab}]")
            fc.append(f"[{out_lab}]format=yuv420p[{out_lab}y]")
            current_v = f"{out_lab}y"

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
                ts_a = float(acl.get("timeline_start") or 0)
                try:
                    sp_a = float(acl.get("speed") or 1.0)
                except (TypeError, ValueError):
                    sp_a = 1.0
                if sp_a <= 1e-6:
                    sp_a = 1.0
                if du_a <= 0 and se_a > ss_a:
                    du_a = max(0.05, (se_a - ss_a) / sp_a)
                elif du_a <= 0:
                    du_a = 0.1
                need_src = max(0.05, du_a * sp_a)
                if se_a <= ss_a:
                    se_a = ss_a + need_src
                elif (se_a - ss_a) > need_src + 0.08:
                    se_a = ss_a + need_src
                src_len = max(1e-3, float(se_a) - float(ss_a))
                _, af_tl_spd = SpeedManager().build_speed_filter(sp_a)
                out_dur = max(0.05, float(du_a))
                if master_av_dur > 0 and ts_a < master_av_dur - 1e-6:
                    out_dur = min(out_dur, max(0.05, master_av_dur - ts_a))
                wall_after_tempo = src_len / sp_a
                want_loop = bool(acl.get("loop", False))
                use_aloop = want_loop and out_dur > wall_after_tempo + 0.02
                vol_fade_a = self._afb.build_volume_fade_filters(acl, out_dur)
                delay_ms_a = max(0, int(round(ts_a * 1000)))
                alab_tl = f"tlau{ai}"
                chain_tl = f"[{aidx}:a]atrim=start={ss_a}:end={se_a},asetpts=PTS-STARTPTS"
                if af_tl_spd:
                    chain_tl += f",{af_tl_spd}"
                chain_tl += ",aresample=48000"
                if use_aloop:
                    chain_tl += f",aloop=loop=-1:size=0,atrim=0:{out_dur:.6f},asetpts=PTS-STARTPTS"
                elif wall_after_tempo > out_dur + 0.02:
                    chain_tl += f",atrim=0:{out_dur:.6f},asetpts=PTS-STARTPTS"
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

        if master_av_dur > 0.05:
            a_cap = "aoutcap"
            fc.append(f"[{final_audio}]atrim=0:{master_av_dur:.6f},asetpts=PTS-STARTPTS[{a_cap}]")
            final_audio = a_cap

        # Tránh FFmpeg chờ/đọc stdin (treo trên Windows khi subprocess không nối stdin).
        # stats_period: ghi progress ra stderr thường xuyên hơn → UI không «im lặng» khi encode lâu.
        args: list[str] = [ff, "-nostdin", "-y", "-hide_banner"]
        try:
            _sp = str(os.environ.get("TOOLFB_FFMPEG_STATS_PERIOD", "0.25") or "0.25").strip()
            if _sp and _sp not in {"0", "off", "no"}:
                float(_sp)  # validate
                args.extend(["-stats_period", _sp])
        except (TypeError, ValueError):
            args.extend(["-stats_period", "0.25"])
        # Khi video đã được ffmpeg tự xoay theo metadata rồi filter lại transpose/hflip → có thể sai góc.
        # Bật: TOOLFB_INPUT_NO_AUTOROTATE=1 (pixel trong filter = đúng orientation trong file; cần chỉnh xoay trong project cho file có tag xoay).
        input_no_autorotate = str(os.environ.get("TOOLFB_INPUT_NO_AUTOROTATE", "0") or "0").strip().lower() in (
            "1",
            "true",
            "yes",
            "on",
        )
        for typ, val, meta in inputs:
            if typ == "file":
                if meta.get("stream_loop") is not None:
                    args.extend(["-stream_loop", str(meta["stream_loop"])])
                if input_no_autorotate:
                    args.extend(["-noautorotate", "-i", _norm_os_path(val)])
                else:
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
        use_faststart = mp4_faststart
        if use_faststart is None:
            use_faststart = str(os.environ.get("TOOLFB_EXPORT_MP4_FASTSTART", "1") or "1").strip().lower() not in (
                "0",
                "false",
                "off",
                "no",
            )
        enc_tail: list[str] = [
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
            "-b:a",
            str(os.environ.get("TOOLFB_EXPORT_AUDIO_BITRATE", "96k") or "96k"),
        ]
        if use_faststart:
            enc_tail.extend(["-movflags", "+faststart"])
        args.extend(enc_tail)
        # VBV + CRF có thể làm encoder «vật lộn» với bitrate → mặc định TẮT để ưu tiên tốc độ.
        # Bật lại: TOOLFB_EXPORT_VBV=1 hoặc đặt TOOLFB_EXPORT_MAXRATE_K (kilo-bit/s).
        vbv_on = str(os.environ.get("TOOLFB_EXPORT_VBV", "0") or "0").strip().lower() in ("1", "true", "yes", "on")
        maxrate_env = str(os.environ.get("TOOLFB_EXPORT_MAXRATE_K", "") or "").strip()
        if lightweight_mode and (
            vbv_on or maxrate_env
        ) and ("264" in vcodec.lower() or "265" in vcodec.lower() or "hevc" in vcodec.lower()):
            px = max(1, int(w) * int(h))
            if px <= 1280 * 720:
                default_maxrate_k = 1400
            elif px <= 1920 * 1080:
                default_maxrate_k = 2400
            elif px <= 2560 * 1440:
                default_maxrate_k = 3800
            else:
                default_maxrate_k = 5200
            try:
                maxrate_k = int(float(maxrate_env or str(default_maxrate_k)))
            except (TypeError, ValueError):
                maxrate_k = default_maxrate_k
            try:
                bufsize_k = int(
                    float(
                        str(os.environ.get("TOOLFB_EXPORT_BUFSIZE_K", str(max(1000, maxrate_k * 2))) or max(1000, maxrate_k * 2))
                    )
                )
            except (TypeError, ValueError):
                bufsize_k = max(1000, maxrate_k * 2)
            if maxrate_k > 0:
                args.extend(["-maxrate", f"{maxrate_k}k"])
            if bufsize_k > 0:
                args.extend(["-bufsize", f"{bufsize_k}k"])
        # Thread: để trống env = tự chọn theo CPU (có trần để tránh spike).
        raw_threads = str(enc_threads if enc_threads is not None else (os.environ.get("TOOLFB_EXPORT_THREADS", "")) or "").strip()
        if not raw_threads:
            try:
                cpu = int(os.cpu_count() or 4)
            except (TypeError, ValueError):
                cpu = 4
            max_threads = max(2, min(12, cpu))
        else:
            try:
                max_threads = int(float(raw_threads))
            except (TypeError, ValueError):
                max_threads = 4
        low_mem = str(os.environ.get("TOOLFB_LOW_MEMORY", "0") or "0").strip().lower() in ("1", "true", "yes", "on")
        if low_mem and max_threads > 0:
            max_threads = max(1, min(4, int(max_threads)))
        if max_threads > 0:
            args.extend(["-threads", str(max_threads)])
        if output_duration_limit_sec is not None and float(output_duration_limit_sec) > 0:
            args.extend(["-t", f"{float(output_duration_limit_sec):.4f}"])
        else:
            cap_out = str(os.environ.get("TOOLFB_EXPORT_CAP_OUTPUT_DUR", "1") or "1").strip().lower() not in (
                "0",
                "false",
                "off",
                "no",
            )
            if cap_out:
                try:
                    pdur = float(master_av_dur)
                except (TypeError, ValueError):
                    pdur = 0.0
                if pdur <= 0.05:
                    pdur = compute_video_timeline_end(project)
                if pdur > 0.05:
                    args.extend(["-t", f"{pdur + 0.08:.4f}"])
        args.append(out)

        return args
