#!/usr/bin/env python3
"""
Video Analyzer Worker
Processes YouTube URLs or local video files
Outputs JSON to stdout for Node.js to parse

Usage:
    python video_analyzer.py <video_source> [language_hint]
    
Arguments:
    video_source: YouTube URL or local file path
    language_hint: Language for Whisper STT (default: vi)
                   Options: vi, en, auto
"""

import sys
import json
import os
import tempfile
import subprocess
from pathlib import Path
from typing import List, Dict, Any, Optional

def get_ffmpeg_path():
    """Get ffmpeg binary path from env var or fall back to bare command"""
    return os.environ.get('FFMPEG_PATH', 'ffmpeg')

def get_ffprobe_path():
    """Get ffprobe binary path from env var or fall back to bare command"""
    return os.environ.get('FFPROBE_PATH', 'ffprobe')

def log(message: str):
    """Print to stderr for logging (stdout reserved for JSON output)"""
    print(message, file=sys.stderr, flush=True)

def progress(step: str, current: int = 0, total: int = 0):
    """Send progress updates to stderr"""
    if total > 0:
        percent = int((current / total) * 100)
        print(f"[PROGRESS] {step}: {current}/{total} ({percent}%)", file=sys.stderr, flush=True)
    else:
        print(f"[PROGRESS] {step}", file=sys.stderr, flush=True)


def download_or_validate_video(source: str, audio_only: bool = False) -> str:
    """
    Download YouTube video/audio or validate local file
    If audio_only=True, downloads only audio (much faster and smaller)
    Returns path to video/audio file
    """
    # Check if it's a YouTube URL
    if "youtube.com" in source or "youtu.be" in source:
        temp_dir = tempfile.mkdtemp(prefix="video_analyzer_")
        
        if audio_only:
            log(f"🎵 Downloading audio only from: {source}")
            # Don't add .mp3 extension - FFmpegExtractAudio will add it automatically
            output_path = os.path.join(temp_dir, "audio")
        else:
            log(f"📥 Downloading YouTube video: {source}")
            output_path = os.path.join(temp_dir, "video.mp4")
        
        # Use yt-dlp Python module
        try:
            import yt_dlp
            
            def progress_hook(d):
                if d['status'] == 'downloading':
                    if 'total_bytes' in d:
                        downloaded = d.get('downloaded_bytes', 0)
                        total = d['total_bytes']
                        if audio_only:
                            progress("Downloading audio", downloaded, total)
                        else:
                            progress("Downloading video", downloaded, total)
                elif d['status'] == 'finished':
                    progress("Download complete")
            
            if audio_only:
                # Download audio only (much faster)
                ydl_opts = {
                    'format': 'bestaudio/best',
                    'outtmpl': output_path,
                    'quiet': True,
                    'no_warnings': True,
                    'noprogress': True,
                    'progress_hooks': [progress_hook],
                    'postprocessors': [{
                        'key': 'FFmpegExtractAudio',
                        'preferredcodec': 'mp3',
                        'preferredquality': '192',
                    }],
                }
            else:
                # Download full video
                ydl_opts = {
                    'format': 'best[ext=mp4]/best',
                    'outtmpl': output_path,
                    'quiet': True,
                    'no_warnings': True,
                    'noprogress': True,
                    'progress_hooks': [progress_hook],
                }
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([source])
            
            # FFmpegExtractAudio adds .mp3 extension automatically
            if audio_only:
                output_path = output_path + '.mp3'
            
            log(f"✅ Downloaded to: {output_path}")
            return output_path
        except Exception as e:
            raise ValueError(f"Failed to download from YouTube: {str(e)}")
    
    # Otherwise, treat as local file path
    elif os.path.isfile(source):
        log(f"📂 Using local file: {source}")
        return source
    
    else:
        raise ValueError(f"Invalid video source: {source}")


def detect_scenes(video_path: str, threshold: float = 30.0, min_scene_duration: float = 10.0) -> List[Dict[str, Any]]:
    """
    Detect scene changes using PySceneDetect
    Then merge short scenes (< min_scene_duration) with next scene
    Returns list of scenes with startSec and endSec
    """
    progress("Detecting scenes")
    log(f"🎬 Detecting scenes in {video_path}...")
    
    from scenedetect import VideoManager, SceneManager
    from scenedetect.detectors import ContentDetector
    
    video_manager = VideoManager([video_path])
    scene_manager = SceneManager()
    scene_manager.add_detector(ContentDetector(threshold=threshold))
    
    # Start video manager
    video_manager.set_downscale_factor()
    video_manager.start()
    
    # Detect scenes
    scene_manager.detect_scenes(frame_source=video_manager)
    scene_list = scene_manager.get_scene_list()
    
    # Convert to JSON-friendly format
    raw_scenes = []
    for index, (start_time, end_time) in enumerate(scene_list, start=1):
        raw_scenes.append({
            "id": f"scene_{index:03d}",
            "index": index,
            "startSec": round(start_time.get_seconds(), 2),
            "endSec": round(end_time.get_seconds(), 2),
        })
    
    log(f"✅ Detected {len(raw_scenes)} raw scenes (before merging)")
    
    # Merge short scenes with next scene
    # A scene should be at least min_scene_duration seconds long
    # Short scenes will be merged into the next scene
    scenes = []
    i = 0
    while i < len(raw_scenes):
        current_scene = raw_scenes[i].copy()
        duration = current_scene["endSec"] - current_scene["startSec"]
        
        # If scene is too short, merge with next scene(s) until we reach min_duration
        if duration < min_scene_duration and i < len(raw_scenes) - 1:
            # Merge with next scenes until we have enough duration
            merged_count = 1
            while (current_scene["endSec"] - current_scene["startSec"]) < min_scene_duration and (i + merged_count) < len(raw_scenes):
                next_scene = raw_scenes[i + merged_count]
                current_scene["endSec"] = next_scene["endSec"]
                merged_count += 1
            
            # Update scene ID and index
            current_scene["id"] = f"scene_{len(scenes) + 1:03d}"
            current_scene["index"] = len(scenes)
            scenes.append(current_scene)
            
            # Skip the merged scenes
            i += merged_count
            log(f"   Merged {merged_count} short scenes into one (duration: {current_scene['endSec'] - current_scene['startSec']:.1f}s)")
        else:
            # Scene is long enough, keep it as is
            current_scene["id"] = f"scene_{len(scenes) + 1:03d}"
            current_scene["index"] = len(scenes)
            scenes.append(current_scene)
            i += 1
    
    log(f"✅ Final: {len(scenes)} scenes after merging (min duration: {min_scene_duration}s)")
    progress(f"Scene detection complete: {len(scenes)} scenes found")
    return scenes


def extract_keyframes(video_path: str, scenes: List[Dict[str, Any]], session_id: str):
    """
    Extract 5 keyframes from special moments in each scene
    Saves to client temp directory per session
    """
    progress("Extracting keyframes")
    log(f"📸 Extracting 5 frames per scene...")
    
    # Use client temp directory from environment variable, or fallback to default
    client_temp_dir = os.environ.get('CLIENT_TEMP_DIR')
    if client_temp_dir:
        session_dir = Path(client_temp_dir) / "frames"
    else:
        # Fallback to old location if env var not set
        home = Path.home()
        session_dir = home / "Library" / "Application Support" / "EgTools" / "video-analysis" / session_id / "frames"
    
    session_dir.mkdir(parents=True, exist_ok=True)
    log(f"📂 Saving frames to: {session_dir}")
    
    total_scenes = len(scenes)
    for idx, scene in enumerate(scenes, 1):
        progress(f"Extracting keyframes", idx, total_scenes)
        
        start_sec = scene["startSec"]
        end_sec = scene["endSec"]
        duration = end_sec - start_sec
        
        # Calculate 5 timestamps: start + 20% + 40% + 60% + 80% of duration
        # This captures special moments throughout the scene
        timestamps = [
            start_sec + (duration * 0.1),   # 10% into scene
            start_sec + (duration * 0.3),   # 30% into scene
            start_sec + (duration * 0.5),   # Middle (50%)
            start_sec + (duration * 0.7),   # 70% into scene
            start_sec + (duration * 0.9),   # 90% into scene
        ]
        
        frame_paths = []
        
        # Extract 5 frames at different timestamps
        for frame_idx, timestamp in enumerate(timestamps, 1):
            frame_path = session_dir / f"frame_{scene['id']}_{frame_idx}.jpg"
            
            try:
                subprocess.run([
                    get_ffmpeg_path(),
                    "-y",  # Overwrite
                    "-ss", str(timestamp),  # Seek to timestamp
                    "-i", video_path,
                    "-frames:v", "1",  # Extract 1 frame
                    "-q:v", "2",  # High quality
                    str(frame_path)
                ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                
                frame_paths.append(str(frame_path))
            except subprocess.CalledProcessError as e:
                log(f"⚠️  Failed to extract frame {frame_idx} for {scene['id']}: {e}")
        
        # Save ABSOLUTE paths (easier for Gemini API to load)
        # Keep frame_paths as absolute paths
        absolute_frame_paths = frame_paths if frame_paths else []
        absolute_keyframe_path = absolute_frame_paths[0] if absolute_frame_paths else None
        
        # Save both framePaths (new) and keyframePath (legacy - first frame)
        # Store as ABSOLUTE paths for easier access
        scene["framePaths"] = absolute_frame_paths
        scene["keyframePath"] = absolute_keyframe_path
        
        log(f"   Scene {scene['id']}: extracted {len(frame_paths)}/5 frames")
    
    total_frames = sum(len(s.get('framePaths', [])) for s in scenes)
    log(f"✅ Extracted {total_frames} frames ({total_frames // 5} scenes × 5 frames)")
    progress(f"Keyframe extraction complete: {total_frames} frames")


def extract_and_transcribe(video_path: str, language: str = "vi", is_audio: bool = False) -> List[Dict[str, Any]]:
    """
    Extract audio from video and transcribe with WhisperX
    If is_audio=True, assumes video_path is already an audio file
    Returns list of transcript segments with timestamps
    """
    
    if is_audio:
        # Already audio file (mp3), convert to WAV for WhisperX
        log(f"🎤 Converting audio to WAV format...")
        progress("Converting audio format")
        audio_path = video_path
        
        # Convert to WAV for WhisperX
        import tempfile
        temp_audio = tempfile.NamedTemporaryFile(suffix=".wav", delete=False)
        wav_path = temp_audio.name
        temp_audio.close()
        
        try:
            result = subprocess.run([
                get_ffmpeg_path(),
                "-y",
                "-i", audio_path,
                "-acodec", "pcm_s16le",  # WAV format
                "-ar", "16000",  # 16kHz sample rate (WhisperX requirement)
                "-ac", "1",  # Mono
                wav_path
            ], check=True, capture_output=True, text=True)
            
            log(f"✅ Audio converted to WAV: {wav_path}")
            audio_for_whisper = wav_path
        except subprocess.CalledProcessError as e:
            log(f"⚠️  Failed to convert audio: {e}")
            log(f"   FFmpeg stdout: {e.stdout}")
            log(f"   FFmpeg stderr: {e.stderr}")
            log(f"   Audio path exists: {os.path.exists(audio_path)}")
            log(f"   Audio path: {audio_path}")
            return []
    else:
        # Extract audio from video
        progress("Extracting audio")
        log(f"🎤 Extracting audio from video...")
        
        # Extract audio to temp file
        import tempfile
        temp_audio = tempfile.NamedTemporaryFile(suffix=".wav", delete=False)
        audio_for_whisper = temp_audio.name
        temp_audio.close()
        
        try:
            result = subprocess.run([
                get_ffmpeg_path(),
                "-y",
                "-i", video_path,
                "-vn",  # No video
                "-acodec", "pcm_s16le",  # WAV format
                "-ar", "16000",  # 16kHz sample rate (WhisperX requirement)
                "-ac", "1",  # Mono
                audio_for_whisper
            ], check=True, capture_output=True, text=True)
            
            log(f"✅ Audio extracted to WAV: {audio_for_whisper}")
        except subprocess.CalledProcessError as e:
            log(f"⚠️  Failed to extract audio: {e}")
            log(f"   FFmpeg stdout: {e.stdout}")
            log(f"   FFmpeg stderr: {e.stderr}")
            log(f"   Video path exists: {os.path.exists(video_path)}")
            log(f"   Video path: {video_path}")
            return []
    
    progress(f"Transcribing audio (language: {language})")
    log(f"🗣️  Transcribing audio (language: {language}, model: base)...")
    
    try:
        import torch
        import whisperx
        
        # Auto-detect device
        device = "cuda" if torch.cuda.is_available() else "cpu"
        compute_type = "float16" if device == "cuda" else "int8"
        
        log(f"📊 Using device: {device}, compute_type: {compute_type}")
        
        # Load WhisperX model (base model for speed)
        progress("✍️ Loading WhisperX model (0%)")
        model = whisperx.load_model("base", device, compute_type=compute_type)
        
        # Load audio
        progress("✍️ Loading audio (10%)")
        audio = whisperx.load_audio(audio_for_whisper)
        
        # Transcribe with progress updates
        progress("✍️ Transcribing audio (30%)")
        
        # Run transcription
        lang = None if language == "auto" else language
        result = model.transcribe(audio, batch_size=16, language=lang)
        
        progress("✍️ Transcribing audio (100%)")
        
        detected_language = result.get("language", language)
        log(f"📊 WhisperX detected language: {detected_language}")
        log(f"📊 WhisperX segments count: {len(result.get('segments', []))}")
        
        # Format segments
        segments = []
        for seg in result.get("segments", []):
            segment_text = seg.get("text", "").strip()
            if segment_text:  # Only add non-empty segments
                segments.append({
                    "startSec": round(seg.get("start", 0), 2),
                    "endSec": round(seg.get("end", 0), 2),
                    "text": segment_text
                })
        
        log(f"✅ Transcribed {len(segments)} segments")
        
        if len(segments) == 0:
            log(f"⚠️  WARNING: WhisperX returned 0 segments! Raw result: {result}")
        
        # Clean up temp audio
        try:
            os.unlink(audio_for_whisper)
        except:
            pass
        
        return segments
        
    except Exception as e:
        log(f"⚠️  Transcription failed: {e}")
        import traceback
        log(f"   Traceback: {traceback.format_exc()}")
        # Clean up temp audio
        try:
            os.unlink(audio_for_whisper)
        except:
            pass
        return []


def map_transcript_to_scenes(scenes: List[Dict[str, Any]], transcript_segments: List[Dict[str, Any]]):
    """
    Map transcript segments to scenes based on overlapping timestamps
    Updates each scene dict with transcriptSegments and transcriptText
    """
    progress("Mapping transcript to scenes")
    log(f"🔗 Mapping transcript to scenes...")
    
    for scene in scenes:
        scene_segments = []
        
        # Find all transcript segments that overlap with this scene
        for seg in transcript_segments:
            # Check if segment overlaps with scene
            if seg["endSec"] > scene["startSec"] and seg["startSec"] < scene["endSec"]:
                scene_segments.append(seg)
        
        scene["transcriptSegments"] = scene_segments
        scene["transcriptText"] = " ".join(s["text"] for s in scene_segments).strip()
    
    log(f"✅ Mapped transcript to {len(scenes)} scenes")
    progress(f"Transcript mapping complete: {len(scenes)} scenes")


def get_video_duration(video_path: str) -> float:
    """Get video duration in seconds using ffprobe, with ffmpeg -i fallback"""
    import re

    # Try ffprobe first
    try:
        cmd = [
            get_ffprobe_path(),
            '-v', 'error',
            '-show_entries', 'format=duration',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            video_path
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        duration = float(result.stdout.strip())
        if duration > 0:
            return duration
    except Exception as e:
        log(f"⚠️  ffprobe failed: {e}")

    # Fallback: parse duration from ffmpeg -i stderr
    try:
        log("🔄 Trying ffmpeg -i fallback for duration...")
        result = subprocess.run(
            [get_ffmpeg_path(), "-i", video_path],
            capture_output=True, text=True
        )
        match = re.search(r'Duration:\s*(\d+):(\d+):(\d+(?:\.\d+)?)', result.stderr)
        if match:
            h, m, s = float(match.group(1)), float(match.group(2)), float(match.group(3))
            duration = h * 3600 + m * 60 + s
            log(f"✅ Got duration via ffmpeg -i: {duration:.2f}s")
            return duration
    except Exception as e:
        log(f"⚠️  ffmpeg -i fallback also failed: {e}")

    log("⚠️  Could not get video duration from any method")
    return 0.0

def get_video_metadata(video_path: str, source: str) -> Dict[str, Any]:
    """
    Get video metadata (duration, title, etc.)
    """
    # Get duration using ffprobe
    try:
        result = subprocess.run([
            get_ffprobe_path(),
            "-v", "quiet",
            "-print_format", "json",
            "-show_format",
            video_path
        ], capture_output=True, text=True, check=True)
        
        probe_data = json.loads(result.stdout)
        duration = float(probe_data["format"]["duration"])
        
        # Extract title from metadata or filename
        title = None
        if "tags" in probe_data.get("format", {}):
            title = probe_data["format"]["tags"].get("title")
        
        if not title:
            title = Path(video_path).stem
        
        return {
            "sourceUrl": source,
            "title": title,
            "durationSec": round(duration, 2)
        }
    except Exception as e:
        log(f"⚠️  Failed to get video metadata: {e}")
        return {
            "sourceUrl": source,
            "title": "Unknown Video",
            "durationSec": 0
        }


def main():
    if len(sys.argv) < 2:
        error_output = {"error": "Usage: video_analyzer.py <mode> [args...]"}
        print(json.dumps(error_output), file=sys.stderr)
        sys.exit(1)
    
    mode = sys.argv[1]
    
    # Handle different modes
    if mode == "download-only":
        # Mode: download-only <youtube_url> <output_path>
        if len(sys.argv) < 4:
            error_output = {"error": "Usage: video_analyzer.py download-only <youtube_url> <output_path>"}
            print(json.dumps(error_output), file=sys.stderr)
            sys.exit(1)
        
        youtube_url = sys.argv[2]
        output_path = sys.argv[3]
        
        try:
            log("📥 Downloading YouTube video...")
            video_path = download_or_validate_video(youtube_url)
            
            # Move to desired output path
            import shutil
            shutil.move(video_path, output_path)
            
            # Get duration
            duration = get_video_duration(output_path)
            
            output = {
                "success": True,
                "videoPath": output_path,
                "duration": duration
            }
            print(json.dumps(output))
            sys.exit(0)
        except Exception as e:
            error_output = {"error": str(e)}
            print(json.dumps(error_output), file=sys.stderr)
            sys.exit(1)
    
    elif mode == "transcribe-only":
        # Mode: transcribe-only <video_url_or_path> <language>
        if len(sys.argv) < 4:
            error_output = {"error": "Usage: video_analyzer.py transcribe-only <video_url_or_path> <language>"}
            print(json.dumps(error_output), file=sys.stderr)
            sys.exit(1)
        
        source = sys.argv[2]
        language_hint = sys.argv[3]
        
        try:
            log("🚀 Transcribe-Only Mode Started")
            log(f"   Source: {source}")
            log(f"   Language: {language_hint}")
            log("")
            
            # Check if source is YouTube URL - download audio only for faster processing
            is_youtube = "youtube.com" in source or "youtu.be" in source
            
            # Step 1: Download audio (if YouTube) or validate video/audio file
            progress("Preparing audio")
            if is_youtube:
                # Download audio only (much faster than video)
                audio_path = download_or_validate_video(source, audio_only=True)
                is_audio_file = True
            else:
                # Local file - could be video or audio
                file_path = download_or_validate_video(source, audio_only=False)
                # Check if it's audio file by extension
                is_audio_file = file_path.lower().endswith(('.mp3', '.wav', '.m4a', '.aac', '.ogg', '.opus'))
                audio_path = file_path
            
            # Step 2: Transcribe (with or without audio extraction)
            if is_audio_file:
                progress("Transcribing audio file")
            else:
                progress("Extracting audio from video")
            
            transcript = extract_and_transcribe(audio_path, language_hint, is_audio=is_audio_file)
            
            # Step 3: Get metadata
            progress("Getting metadata")
            if is_youtube:
                # For YouTube, get metadata from the audio file path (contains video info)
                metadata = get_video_metadata(audio_path, source)
            else:
                metadata = get_video_metadata(audio_path, source)
            
            # Step 4: Output JSON to stdout
            progress("Generating output")
            output = {
                "video": metadata,
                "transcript": transcript
            }
            
            log("")
            log("✅ Transcription complete!")
            log(f"   Source: {metadata.get('title', 'Unknown')}")
            log(f"   Duration: {metadata.get('durationSec', 0)}s")
            log(f"   Transcript segments: {len(transcript)}")
            log(f"   Method: {'Audio-only download' if is_youtube else 'Local file'}")
            log("")
            progress("Complete!")
            
            # Print JSON to stdout
            print(json.dumps(output, ensure_ascii=False, indent=2))
            sys.exit(0)
        except Exception as e:
            error_output = {"error": str(e)}
            log(f"❌ Error: {e}")
            print(json.dumps(error_output), file=sys.stderr)
            sys.exit(1)
    
    elif mode == "split-only":
        # Mode: split-only <video_path> <save_folder> <method> [interval]
        if len(sys.argv) < 5:
            error_output = {"error": "Usage: video_analyzer.py split-only <video_path> <save_folder> <method> [interval]"}
            print(json.dumps(error_output), file=sys.stderr)
            sys.exit(1)
        
        video_path = sys.argv[2]
        save_folder = sys.argv[3]
        split_method = sys.argv[4]  # 'auto' or 'manual'
        interval = int(sys.argv[5]) if len(sys.argv) > 5 else 8
        
        try:
            if split_method == 'auto':
                log("🎬 Auto detecting scenes with PySceneDetect...")
                scenes = detect_scenes(video_path)
                
                # Split video by scenes
                segments = []
                for i, scene in enumerate(scenes):
                    segment_path = os.path.join(save_folder, f"segment_{i:03d}.mp4")
                    
                    # Use ffmpeg to extract segment
                    # Re-encode to avoid black frames at start (keyframe issues with -c copy)
                    ffmpeg_cmd = [
                        get_ffmpeg_path(), '-i', video_path,
                        '-ss', str(scene['startSec']),
                        '-t', str(scene['endSec'] - scene['startSec']),
                        '-c:v', 'libx264',
                        '-c:a', 'aac',
                        '-preset', 'fast',
                        '-crf', '23',
                        '-y', segment_path
                    ]
                    subprocess.run(ffmpeg_cmd, check=True, capture_output=True)
                    
                    segments.append({
                        "index": i,
                        "startTime": scene['startSec'],
                        "endTime": scene['endSec'],
                        "duration": scene['endSec'] - scene['startSec'],
                        "videoPath": segment_path
                    })
            else:
                # Manual split by interval
                log(f"✂️  Splitting video every {interval}s...")
                duration = get_video_duration(video_path)
                segment_count = int(duration / interval) + (1 if duration % interval > 0 else 0)
                
                segments = []
                for i in range(segment_count):
                    start_time = i * interval
                    end_time = min((i + 1) * interval, duration)
                    segment_path = os.path.join(save_folder, f"segment_{i:03d}.mp4")
                    
                    # Use ffmpeg to extract segment
                    # Re-encode to avoid black frames at start (keyframe issues with -c copy)
                    ffmpeg_cmd = [
                        get_ffmpeg_path(), '-i', video_path,
                        '-ss', str(start_time),
                        '-t', str(end_time - start_time),
                        '-c:v', 'libx264',
                        '-c:a', 'aac',
                        '-preset', 'fast',
                        '-crf', '23',
                        '-y', segment_path
                    ]
                    subprocess.run(ffmpeg_cmd, check=True, capture_output=True)
                    
                    segments.append({
                        "index": i,
                        "startTime": start_time,
                        "endTime": end_time,
                        "duration": end_time - start_time,
                        "videoPath": segment_path
                    })
            
            output = {
                "success": True,
                "segments": segments
            }
            print(json.dumps(output))
            sys.exit(0)
        except Exception as e:
            error_output = {"error": str(e)}
            print(json.dumps(error_output), file=sys.stderr)
            sys.exit(1)
    
    # Original full analysis mode
    elif len(sys.argv) >= 3:
        source = sys.argv[1]
        language_hint = sys.argv[2]
        session_id = sys.argv[3] if len(sys.argv) > 3 else "temp"
    else:
        error_output = {"error": "Invalid mode or arguments"}
        print(json.dumps(error_output), file=sys.stderr)
        sys.exit(1)
    
    try:
        log("🚀 Video Analyzer Worker Started")
        log(f"   Source: {source}")
        log(f"   Language: {language_hint}")
        log("")
        
        # Step 1: Download or validate video
        progress("Preparing video")
        video_path = download_or_validate_video(source)
        
        # Step 2: Detect scenes
        progress("Detecting scenes")
        scenes = detect_scenes(video_path)
        
        # Step 3: Extract 5 frames per scene
        progress("Extracting frames (5 per scene)")
        extract_keyframes(video_path, scenes, session_id)
        
        # Step 4: Extract audio & transcribe
        progress("Extracting audio")
        transcript = extract_and_transcribe(video_path, language_hint)
        
        # Step 5: Map transcript to scenes
        progress("Mapping transcript to scenes")
        map_transcript_to_scenes(scenes, transcript)
        
        # Step 6: Get video metadata
        progress("Getting video metadata")
        metadata = get_video_metadata(video_path, source)
        
        # Step 7: Output JSON to stdout
        progress("Generating output")
        output = {
            "video": metadata,
            "scenes": scenes
        }
        
        log("")
        log("✅ Analysis complete!")
        log(f"   Video: {metadata['title']}")
        log(f"   Duration: {metadata['durationSec']}s")
        log(f"   Scenes: {len(scenes)}")
        log(f"   Transcript segments: {len(transcript)}")
        log("")
        progress("Complete!")
        
        # Print JSON to stdout (Node.js will parse this)
        # This MUST be the ONLY thing printed to stdout!
        print(json.dumps(output, ensure_ascii=False, indent=2))
        sys.exit(0)
        
    except Exception as e:
        error_output = {"error": str(e)}
        log(f"❌ Error: {e}")
        print(json.dumps(error_output), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

