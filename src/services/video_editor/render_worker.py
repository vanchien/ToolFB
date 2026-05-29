"""Render FFmpeg trong thread; parse progress từ stderr."""

from __future__ import annotations

import os
import re
import subprocess
import threading
import time
import uuid
from collections import deque
from pathlib import Path
from typing import Any, Callable

from src.services.video_editor.layout import ensure_video_editor_layout


ProgressCb = Callable[[float], None]
WaitHeartbeatCb = Callable[[], None]

# Giới hạn số FFmpeg chạy song song (preview + export) — mặc định 2 (``TOOLFB_FFMPEG_CONCURRENCY``).
_FFMPEG_SLOTS: threading.Semaphore | None = None
_FFMPEG_SLOTS_GUARD = threading.Lock()


def _ffmpeg_slot_count() -> int:
    raw = str(os.environ.get("TOOLFB_FFMPEG_CONCURRENCY", "2") or "2").strip()
    try:
        return max(1, min(3, int(raw)))
    except ValueError:
        return 2


def _ffmpeg_slots() -> threading.Semaphore:
    global _FFMPEG_SLOTS
    with _FFMPEG_SLOTS_GUARD:
        if _FFMPEG_SLOTS is None:
            _FFMPEG_SLOTS = threading.Semaphore(_ffmpeg_slot_count())
        return _FFMPEG_SLOTS


class RenderWorker:
    """Render video bằng FFmpeg trong thread riêng."""

    _OUT_TIME_MS_RE = re.compile(r"out_time_ms=(\d+)")
    _TIME_EQ_RE = re.compile(r"time=(\d+):(\d+):(\d+(?:\.\d+)?)")

    @staticmethod
    def _rewrite_filter_complex_to_script(command: list[str], *, project_id: str) -> tuple[list[str], Path] | None:
        """
        Tránh WinError 206 trên Windows: thay ``-filter_complex <very long>`` bằng
        ``-filter_complex_script <tmp-file>``.
        """
        try:
            idx = command.index("-filter_complex")
        except ValueError:
            return None
        if idx + 1 >= len(command):
            return None
        fc_expr = str(command[idx + 1] or "").strip()
        if not fc_expr:
            return None
        tmp_dir = ensure_video_editor_layout()["temp"]
        tmp_dir.mkdir(parents=True, exist_ok=True)
        safe = "".join(c for c in str(project_id or "export") if c.isalnum() or c in "-_")[:40] or "export"
        script_path = tmp_dir / f"ffmpeg_fc_{safe}_{uuid.uuid4().hex[:8]}.txt"
        script_path.write_text(fc_expr, encoding="utf-8", errors="replace")
        new_cmd = list(command[:idx]) + ["-filter_complex_script", str(script_path)] + list(command[idx + 2 :])
        return new_cmd, script_path

    def __init__(self) -> None:
        self._cancel_event = threading.Event()
        self._proc_lock = threading.Lock()
        self._active_procs: set[subprocess.Popen[str]] = set()

    def clear_cancel(self) -> None:
        self._cancel_event.clear()

    def is_cancel_requested(self) -> bool:
        return self._cancel_event.is_set()

    def cancel_all(self) -> None:
        """Yêu cầu dừng mọi ffmpeg process đang render."""
        self._cancel_event.set()
        with self._proc_lock:
            procs = list(self._active_procs)
        for p in procs:
            try:
                p.terminate()
            except Exception:
                pass

    def shutdown_active_encoders(self, *, wait_s: float = 1.25) -> None:
        """
        Khi thoát ứng dụng: hủy encode, terminate rồi kill nếu còn sống
        (tránh ffmpeg/ffprobe orphan trên Windows).
        """
        self._cancel_event.set()
        with self._proc_lock:
            procs = list(self._active_procs)
        for p in procs:
            try:
                if p.poll() is None:
                    p.terminate()
            except Exception:
                pass
        deadline = time.monotonic() + max(0.05, float(wait_s))
        while time.monotonic() < deadline:
            alive = [p for p in procs if p.poll() is None]
            if not alive:
                break
            time.sleep(0.05)
        for p in procs:
            try:
                if p.poll() is None:
                    p.kill()
            except Exception:
                pass

    def render(
        self,
        project: dict[str, Any],
        output_path: str,
        command: list[str],
        *,
        duration_sec: float,
        progress_callback: ProgressCb | None = None,
        log_path: Path | None = None,
        wait_heartbeat_callback: WaitHeartbeatCb | None = None,
    ) -> dict[str, Any]:
        """
        Chạy FFmpeg export MP4.
        `command` là argv đầy đủ (ffmpeg đầu tiên).
        """
        from src.utils.concurrency_runtime import workload_scope

        sem = _ffmpeg_slots()
        sem.acquire()
        try:
            with workload_scope("video_editor"):
                return self._render_locked(
                    project,
                    output_path,
                    command,
                    duration_sec=duration_sec,
                    progress_callback=progress_callback,
                    log_path=log_path,
                    wait_heartbeat_callback=wait_heartbeat_callback,
                )
        finally:
            sem.release()

    def _render_locked(
        self,
        project: dict[str, Any],
        output_path: str,
        command: list[str],
        *,
        duration_sec: float,
        progress_callback: ProgressCb | None = None,
        log_path: Path | None = None,
        wait_heartbeat_callback: WaitHeartbeatCb | None = None,
    ) -> dict[str, Any]:
        out: dict[str, Any] = {"ok": False, "error_message": "", "log_file": ""}
        lp = log_path
        if lp is None:
            logs = ensure_video_editor_layout()["logs"]
            pid = str(project.get("id") or "export")
            safe = "".join(c for c in pid if c.isalnum() or c in "-_")[:64]
            lp = logs / f"render_{safe}.log"

        lp.parent.mkdir(parents=True, exist_ok=True)
        try:
            tail_max = int(float(os.environ.get("TOOLFB_FFMPEG_STDERR_TAIL_LINES", "6000") or "6000"))
        except (TypeError, ValueError):
            tail_max = 6000
        tail_max = max(400, min(50_000, tail_max))
        stderr_tail: deque[str] = deque(maxlen=tail_max)

        # Không dùng PIPE cho stdout/stdin: FFmpeg có thể ghi đủ dữ liệu ra stdout làm đầy buffer
        # trong khi thread chỉ đọc stderr → deadlock (treo vĩnh viễn ở «đang ghi file»).
        # stdin DEVNULL tránh một số bản FFmpeg chờ input tương tác.
        popen_kw: dict = {
            "stdin": subprocess.DEVNULL,
            "stdout": subprocess.DEVNULL,
            "stderr": subprocess.PIPE,
            "text": True,
            "encoding": "utf-8",
            "errors": "replace",
        }
        if os.name == "nt" and hasattr(subprocess, "CREATE_NO_WINDOW"):
            flags = int(subprocess.CREATE_NO_WINDOW)
            # Mặc định không hạ ưu tiên (export nhanh hơn). Nếu máy từng quá tải/restart: TOOLFB_EXPORT_LOW_PRIORITY=1
            low_pri = str(os.environ.get("TOOLFB_EXPORT_LOW_PRIORITY", "0") or "0").strip().lower() in (
                "1",
                "true",
                "yes",
                "on",
            )
            if low_pri and hasattr(subprocess, "BELOW_NORMAL_PRIORITY_CLASS"):
                flags |= int(getattr(subprocess, "BELOW_NORMAL_PRIORITY_CLASS"))
            popen_kw["creationflags"] = flags
        if self._cancel_event.is_set():
            out["error_message"] = "Đã dừng export."
            return out
        cleanup_fc_script: Path | None = None
        run_command = list(command)
        try:
            proc = subprocess.Popen(run_command, **popen_kw)
        except OSError as e:
            # Windows command line quá dài (WinError 206): fallback sang filter_complex_script.
            if os.name == "nt" and getattr(e, "winerror", None) == 206:
                rewritten = self._rewrite_filter_complex_to_script(
                    run_command,
                    project_id=str(project.get("id") or "export"),
                )
                if rewritten is not None:
                    run_command, cleanup_fc_script = rewritten
                    try:
                        proc = subprocess.Popen(run_command, **popen_kw)
                    except OSError as e2:
                        out["error_message"] = f"Không chạy được FFmpeg: {e2}"
                        if cleanup_fc_script is not None:
                            cleanup_fc_script.unlink(missing_ok=True)
                        return out
                else:
                    out["error_message"] = f"Không chạy được FFmpeg: {e}"
                    return out
            else:
                out["error_message"] = f"Không chạy được FFmpeg: {e}"
                return out
        with self._proc_lock:
            self._active_procs.add(proc)

        assert proc.stderr is not None
        total_us = max(1.0, float(duration_sec) * 1_000_000.0)

        def _drain_stderr() -> None:
            try:
                for line in proc.stderr:
                    stderr_tail.append(line)
                    if self._cancel_event.is_set():
                        try:
                            proc.terminate()
                        except Exception:
                            pass
                        break
                    if progress_callback and duration_sec > 0:
                        prog = self.parse_progress(line, total_duration_us=total_us)
                        if prog is not None:
                            try:
                                progress_callback(prog)
                            except Exception:
                                pass
            except Exception:
                pass

        stderr_thread = threading.Thread(target=_drain_stderr, name="ffmpeg-stderr", daemon=True)
        stderr_thread.start()
        timed_out = False
        grace_s = 0.0
        try:
            try:
                wait_mult = float(os.environ.get("TOOLFB_FFMPEG_WAIT_MULT", "15") or "15")
            except (TypeError, ValueError):
                wait_mult = 15.0
            try:
                # Sàn thời gian chờ wall-clock: timeline ngắn vẫn có thể mã hóa rất lâu (full HD + filter_complex).
                # Trước đây max(240, …) khiến clip vài giây bị cắt sau ~240s dù file vẫn tăng — oan timeout.
                min_grace = float(os.environ.get("TOOLFB_FFMPEG_MIN_GRACE_S", "3600") or "3600")
            except (TypeError, ValueError):
                min_grace = 3600.0
            min_grace = max(300.0, min(86400.0, min_grace))
            grace_s = max(
                min_grace,
                float(duration_sec) * max(3.0, wait_mult) + 120.0,
            )
            deadline = time.monotonic() + grace_s
            last_hb = time.monotonic()
            try:
                hb_every = float(os.environ.get("TOOLFB_FFMPEG_HEARTBEAT_SEC", "12") or "12")
            except (TypeError, ValueError):
                hb_every = 12.0
            hb_every = max(5.0, min(120.0, hb_every))
            while proc.poll() is None:
                if self._cancel_event.is_set():
                    try:
                        proc.terminate()
                    except Exception:
                        pass
                if time.monotonic() >= deadline:
                    timed_out = True
                    try:
                        proc.terminate()
                    except Exception:
                        pass
                    t_kill = time.monotonic()
                    while proc.poll() is None and (time.monotonic() - t_kill) < 3.0:
                        time.sleep(0.05)
                    if proc.poll() is None:
                        try:
                            proc.kill()
                        except Exception:
                            pass
                    break
                try:
                    proc.wait(timeout=0.5)
                except subprocess.TimeoutExpired:
                    now = time.monotonic()
                    if wait_heartbeat_callback and (now - last_hb) >= hb_every:
                        last_hb = now
                        try:
                            wait_heartbeat_callback()
                        except Exception:
                            pass
        finally:
            # Không đóng stderr từ thread chính trên Windows — có thể race với thread đọc và làm FFmpeg/xuất kẹt oan.
            # Đừng chờ quá lâu ở pha "join stderr": có thể khiến UI đứng ở 100% rất lâu dù clip đã xong.
            # Nếu thread đọc stderr chưa thoát kịp thì bỏ qua (daemon), vòng clip vẫn tiếp tục.
            stderr_thread.join(timeout=3.0)
            with self._proc_lock:
                self._active_procs.discard(proc)
            if cleanup_fc_script is not None:
                cleanup_fc_script.unlink(missing_ok=True)
        if timed_out:
            tail = ("".join(stderr_tail) or "").strip()[-1200:] or "Không có stderr."
            out["error_message"] = (
                f"FFmpeg không thoát sau ~{int(grace_s)}s (timeout). "
                "Nếu dung lượng file vẫn tăng đều, thường là mã hóa còn chạy nhưng hạn chờ quá thấp so với độ phức tạp — "
                "tăng biến môi trường TOOLFB_FFMPEG_MIN_GRACE_S (mặc định 3600) hoặc TOOLFB_FFMPEG_WAIT_MULT. "
                "Nếu không tăng: treo I/O / antivirus quét file đang ghi — thử đổi thư mục output.\n"
                f"Chi tiết (đuôi stderr):\n{tail}"
            )
            return out
        body = "".join(stderr_tail)
        try:
            lp.write_text(body[-400_000:], encoding="utf-8", errors="replace")
        except OSError:
            pass
        out["log_file"] = str(lp)

        if proc.returncode != 0:
            if self._cancel_event.is_set():
                out["error_message"] = "Đã dừng export."
                return out
            tail = body.strip()[-1200:] if body.strip() else "Không có stderr."
            rc = int(proc.returncode)
            hint = ""
            low = body.lower()
            # Windows: 3221225477 / -1073741819 = 0xC0000005 (access violation) — FFmpeg crash, không phải lỗi mã hóa thông thường.
            if os.name == "nt" and rc in (-1073741819, 3221225477):
                hint = (
                    "\n\n(Gợi ý: FFmpeg bị crash bộ nhớ Windows — thử bản FFmpeg build ổn định hoặc giảm filter phức tạp.)"
                )
                if "png" in low or "inflate" in low:
                    hint += (
                        "\nCó dấu hiệu lỗi ảnh PNG (logo/overlay): thử đổi file logo sang PNG khác hoặc JPG, "
                        "hoặc tắt overlay tạm để xác nhận."
                    )
            out["error_message"] = f"FFmpeg lỗi (mã {rc}). Chi tiết:\n{tail}{hint}"
            return out

        outp = Path(output_path).expanduser()
        if not outp.is_file():
            out["error_message"] = f"Export xong nhưng không thấy file: {outp}"
            return out

        out["ok"] = True
        return out

    def render_thread(
        self,
        project: dict[str, Any],
        output_path: str,
        command: list[str],
        *,
        duration_sec: float,
        progress_callback: ProgressCb | None = None,
        done_callback: Callable[[dict[str, Any]], None] | None = None,
        log_path: Path | None = None,
    ) -> None:
        """Chạy render trong thread nền; gọi done_callback khi xong (ở thread đó)."""

        def _run() -> None:
            result = self.render(
                project,
                output_path,
                command,
                duration_sec=duration_sec,
                progress_callback=progress_callback,
                log_path=log_path,
            )
            if done_callback:
                done_callback(result)

        threading.Thread(target=_run, daemon=True).start()

    def parse_progress(self, ffmpeg_line: str, *, total_duration_us: float) -> float | None:
        """
        Parse progress từ stderr FFmpeg (out_time_ms hoặc time=HH:MM:SS.xx).
        Trả về 0..1 hoặc None.
        """
        line = ffmpeg_line or ""
        m = self._OUT_TIME_MS_RE.search(line)
        if m:
            try:
                us = float(m.group(1))
            except ValueError:
                return None
            if total_duration_us <= 0:
                return None
            return max(0.0, min(1.0, us / total_duration_us))
        m2 = self._TIME_EQ_RE.search(line)
        if m2:
            try:
                hh, mm, ss = float(m2.group(1)), float(m2.group(2)), float(m2.group(3))
                sec = hh * 3600 + mm * 60 + ss
                us = sec * 1_000_000.0
            except ValueError:
                return None
            if total_duration_us <= 0:
                return None
            return max(0.0, min(1.0, us / total_duration_us))
        return None
