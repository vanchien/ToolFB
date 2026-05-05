#!/usr/bin/env python3
"""
Frame Extractor Worker
Extracts frames from local video or YouTube URL
Supports:
  - Manual mode: Extract frame every N seconds
  - Scene detection mode: Extract frames at scene changes (using PySceneDetect)

Usage:
    python frame_extractor.py <mode> <video_source> <output_dir> [options_json]
    
Modes:
    download-only     - Download YouTube video only
    extract-interval  - Extract frames at fixed interval
"""

import sys
import json
import os
import subprocess
import time
from pathlib import Path
from typing import List, Dict, Any

def get_ffmpeg_path():
    """Get ffmpeg binary path from env var or fall back to bare command"""
    return os.environ.get('FFMPEG_PATH', 'ffmpeg')

def get_ffprobe_path():
    """Get ffprobe binary path from env var or fall back to bare command"""
    return os.environ.get('FFPROBE_PATH', 'ffprobe')

def log(message: str):
    """Print to stderr for logging (stdout reserved for JSON output)"""
    print(message, file=sys.stderr, flush=True)

def progress(step: str, current: int = 0, total: int = 0, percent: int = 0):
    """Send progress updates to stderr in parseable format"""
    if total > 0:
        percent = int((current / total) * 100)
    print(f"[PROGRESS] {step} ({percent}%)", file=sys.stderr, flush=True)


def download_youtube_video(url: str, output_dir: str, cookies_path: str = None) -> str:
    """
    Download YouTube video using yt-dlp
    Returns path to downloaded video
    """
    log(f"📥 Downloading YouTube video: {url}")
    if cookies_path:
        log(f"🍪 Using cookies from: {cookies_path}")
    progress("Downloading video", percent=0)
    
    try:
        import yt_dlp
        
        # Check ffmpeg availability
        try:
            ffmpeg_version = subprocess.check_output([get_ffmpeg_path(), "-version"], stderr=subprocess.STDOUT).decode().split('\n')[0]
            log(f"🎬 Found ffmpeg: {ffmpeg_version}")
        except FileNotFoundError:
            log("⚠️ ffmpeg not found in PATH! downloading might fail if merging is required.")
        except Exception as e:
            log(f"⚠️ Error checking ffmpeg: {e}")

        # Generate output filename
        # Use a simpler template to avoid issues, we rename later anyway if needed
        output_template = os.path.join(output_dir, f"youtube_{int(time.time())}.%(ext)s")
        
        def progress_hook(d):
            if d['status'] == 'downloading':
                if 'total_bytes' in d:
                    downloaded = d.get('downloaded_bytes', 0)
                    total = d['total_bytes']
                    pct = int((downloaded / total) * 100) if total > 0 else 0
                    progress("Downloading video", percent=pct)
            elif d['status'] == 'finished':
                progress("Download complete", percent=100)
        
        ydl_opts = {
            'format': 'bestvideo+bestaudio/best', # Allow any best quality format
            'outtmpl': output_template,
            'quiet': True,
            'no_warnings': True,
            'noprogress': True,
            'progress_hooks': [progress_hook],
            'ignoreerrors': True,
            'no_playlist': True,
            'nocheckcertificate': True,
            'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        }

        # Add cookies if provided in options (passed via json from caller)
        # We need to access global options somehow, or pass them to this function
        # But download_video doesn't take options yet.
        # Let's inspect where download_video is called.

        
        if cookies_path and os.path.exists(cookies_path):
            ydl_opts['cookiefile'] = cookies_path
            
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            # Get info first to know the filename
            info = ydl.extract_info(url, download=True)
            if not info:
                raise ValueError("Failed to extract video info")
            
            # yt-dlp might change extension based on format
            # We need to find the actual file
            # The info dict has '_filename' or we check the dir
            filename = ydl.prepare_filename(info)
            output_path = filename
            
        log(f"✅ Download finished, checking file: {output_path}")

        # Check if file exists and has size
        if not os.path.exists(output_path):
            # Check if there are other files in the dir (maybe different extension)
            files = os.listdir(output_dir)
            log(f"⚠️ File not found at {output_path}. Directory contents: {files}")
            
            # Helper to find the most recent file
            possible_files = [f for f in files if f.startswith("youtube_") and (f.endswith(".mp4") or f.endswith(".mkv") or f.endswith(".webm"))]
            if possible_files:
                possible_files.sort(key=lambda x: os.path.getmtime(os.path.join(output_dir, x)), reverse=True)
                new_path = os.path.join(output_dir, possible_files[0])
                log(f"🔄 Found alternative file: {new_path}")
                output_path = new_path
            else:
                raise ValueError(f"Download seemed to finish but file is missing: {output_path}")

        if os.path.getsize(output_path) == 0:
             raise ValueError(f"The downloaded file is empty: {output_path}")

        log(f"✅ Downloaded to: {output_path} ({os.path.getsize(output_path)} bytes)")
        return output_path
    
    except Exception as e:
        log(f"❌ Download failed: {str(e)}")
        raise e


def get_video_duration(video_path: str) -> float:
    """Get video duration using ffprobe, with ffmpeg -i fallback"""
    import re

    # Try ffprobe first
    try:
        result = subprocess.run([
            get_ffprobe_path(),
            "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            video_path
        ], capture_output=True, text=True, check=True)
        duration = float(result.stdout.strip())
        if duration > 0:
            return duration
    except Exception as e:
        log(f"⚠️ ffprobe failed: {e}")

    # Fallback: parse duration from ffmpeg -i stderr
    try:
        log("🔄 Trying ffmpeg -i fallback for duration...")
        result = subprocess.run(
            [get_ffmpeg_path(), "-i", video_path],
            capture_output=True, text=True
        )
        # ffmpeg -i outputs to stderr: "Duration: HH:MM:SS.ms"
        match = re.search(r'Duration:\s*(\d+):(\d+):(\d+(?:\.\d+)?)', result.stderr)
        if match:
            h, m, s = float(match.group(1)), float(match.group(2)), float(match.group(3))
            duration = h * 3600 + m * 60 + s
            log(f"✅ Got duration via ffmpeg -i: {duration:.2f}s")
            return duration
    except Exception as e:
        log(f"⚠️ ffmpeg -i fallback also failed: {e}")

    log("⚠️ Could not get video duration from any method")
    return 0


def extract_frames_by_interval(video_path: str, output_dir: str, interval: float) -> List[Dict[str, Any]]:
    """
    Extract frames at fixed intervals
    Returns list of extracted frame info
    """
    log(f"🎞️ Extracting frames every {interval} seconds...")
    
    duration = get_video_duration(video_path)
    if duration <= 0:
        raise ValueError("Could not determine video duration")
    
    num_frames = int(duration / interval) + 1
    frames = []
    
    log(f"📊 Video duration: {duration:.1f}s, will extract ~{num_frames} frames")
    
    for i in range(num_frames):
        timestamp = i * interval
        if timestamp >= duration:
            break
            
        progress(f"Extracting frame {i+1}/{num_frames}", current=i+1, total=num_frames)
        
        frame_filename = f"frame_{i:04d}_{timestamp:.2f}s.png"
        frame_path = os.path.join(output_dir, frame_filename)
        
        try:
            subprocess.run([
                get_ffmpeg_path(),
                "-y",
                "-ss", str(timestamp),
                "-i", video_path,
                "-frames:v", "1",
                "-q:v", "1",  # Highest quality
                frame_path
            ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            
            frames.append({
                "index": i,
                "timestamp": round(timestamp, 2),
                "path": frame_path,
                "filename": frame_filename
            })
            
        except subprocess.CalledProcessError as e:
            log(f"⚠️ Failed to extract frame at {timestamp}s: {e}")
    
    log(f"✅ Extracted {len(frames)} frames")
    return frames


def split_video_segments(video_path: str, output_dir: str, interval: float) -> List[Dict[str, Any]]:
    """
    Split video into segments of fixed duration (with audio).
    Returns list of segment info with paths.
    """
    log(f"✂️ Splitting video into {interval}s segments (with audio)...")

    duration = get_video_duration(video_path)
    if duration <= 0:
        raise ValueError("Could not determine video duration")

    num_segments = int(duration / interval) + (1 if duration % interval > 0.5 else 0)
    segments = []

    log(f"📊 Video duration: {duration:.1f}s, will create ~{num_segments} segments")

    for i in range(num_segments):
        start_time = i * interval
        segment_duration = min(interval, duration - start_time)
        if segment_duration < 0.5:
            break

        progress(f"Splitting segment {i+1}/{num_segments}", current=i+1, total=num_segments)

        segment_filename = f"segment_{i:04d}_{start_time:.0f}s-{start_time + segment_duration:.0f}s.mp4"
        segment_path = os.path.join(output_dir, segment_filename)

        try:
            subprocess.run([
                get_ffmpeg_path(),
                "-y",
                "-ss", str(start_time),
                "-i", video_path,
                "-t", str(segment_duration),
                "-c:v", "libx264",
                "-c:a", "aac",
                "-preset", "fast",
                "-crf", "23",
                segment_path
            ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

            segments.append({
                "index": i,
                "startTime": round(start_time, 2),
                "endTime": round(start_time + segment_duration, 2),
                "duration": round(segment_duration, 2),
                "path": segment_path,
                "filename": segment_filename
            })

        except subprocess.CalledProcessError as e:
            log(f"⚠️ Failed to split segment at {start_time}s: {e}")

    log(f"✅ Created {len(segments)} video segments")
    return segments


def main():
    if len(sys.argv) < 4:
        print("Usage: python frame_extractor.py <mode> <video_source> <output_dir> [options_json]", file=sys.stderr)
        sys.exit(1)
    
    mode = sys.argv[1]
    video_source = sys.argv[2]
    output_dir = sys.argv[3]
    options = json.loads(sys.argv[4]) if len(sys.argv) > 4 else {}
    
    # Ensure output directory exists and is absolute
    output_dir = os.path.abspath(output_dir)
    os.makedirs(output_dir, exist_ok=True)
    
    log(f"📁 Output directory: {output_dir}")
    
    try:
        result = {}
        
        # Check if source is YouTube URL
        is_youtube = "youtube.com" in video_source or "youtu.be" in video_source
        
        if mode == "download-only":
            if not is_youtube:
                raise ValueError("download-only mode requires a YouTube URL")
            
            video_path = download_youtube_video(video_source, output_dir)
            duration = get_video_duration(video_path)
            
            result = {
                "success": True,
                "videoPath": video_path,
                "duration": duration
            }
            
        elif mode == "extract-interval":
            # Download YouTube video if needed
            if is_youtube:
                video_path = download_youtube_video(video_source, output_dir)
            else:
                video_path = video_source
                if not os.path.isfile(video_path):
                    raise ValueError(f"Video file not found: {video_path}")
            
            interval = options.get("interval", 5)  # Default: 5 seconds
            frames = extract_frames_by_interval(video_path, output_dir, interval)
            
            result = {
                "success": True,
                "mode": "interval",
                "videoPath": video_path,
                "interval": interval,
                "frames": frames,
                "frameCount": len(frames)
            }
            
        elif mode == "split-segments":
            # Split video into segments with audio
            if is_youtube:
                cookies_path = options.get("cookiesPath")
                video_path = download_youtube_video(video_source, output_dir, cookies_path)
            else:
                video_path = video_source
                if not os.path.isfile(video_path):
                    raise ValueError(f"Video file not found: {video_path}")

            interval = options.get("interval", 8)  # Default: 8 seconds
            segments = split_video_segments(video_path, output_dir, interval)

            result = {
                "success": True,
                "mode": "split-segments",
                "videoPath": video_path,
                "interval": interval,
                "segments": segments,
                "segmentCount": len(segments)
            }

        else:
            raise ValueError(f"Unknown mode: {mode}")

        # Output result as JSON to stdout
        print(json.dumps(result))
        
    except Exception as e:
        error_result = {
            "success": False,
            "error": str(e)
        }
        print(json.dumps(error_result))
        sys.exit(1)


if __name__ == "__main__":
    main()
