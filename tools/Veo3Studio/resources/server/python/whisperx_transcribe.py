#!/usr/bin/env python3
"""
WhisperX Transcription Script (replaces openai-whisper)
Uses WhisperX for transcription with optional word-level timestamps

Usage: python whisperx_transcribe.py '<json_input>'
Input JSON: {
    "audio_path": "/path/to/audio.mp3",
    "language": "en",
    "output_path": "/path/to/output.json",
    "word_timestamps": true  # optional, default false
}

Output: Transcription result with segments
"""

import sys
import json
import os
import warnings

# Save original stderr before anything else
_original_stderr = sys.stderr

# Suppress all warnings to prevent them from polluting stdout
warnings.filterwarnings("ignore")

# Redirect any unwanted output to stderr before importing heavy libraries
import io
_original_stdout = sys.stdout
sys.stdout = io.StringIO()

# Check for required packages
try:
    import torch
    
    # Fix for PyTorch 2.6+ weights_only default change - MUST be done BEFORE importing whisperx
    # Force weights_only=False for all torch.load calls (WhisperX/pyannote models need this)
    _original_torch_load = torch.load
    def _patched_torch_load(*args, **kwargs):
        # Always force weights_only=False regardless of what caller passes
        kwargs['weights_only'] = False
        return _original_torch_load(*args, **kwargs)
    torch.load = _patched_torch_load
    
    # Now import whisperx after patching torch.load
    import whisperx
except ImportError as e:
    sys.stdout = _original_stdout
    print(json.dumps({
        "success": False,
        "error": f"Required package not installed: {e}. Run: pip install whisperx torch"
    }))
    sys.exit(1)

# Restore stdout
sys.stdout = _original_stdout


def log(message: str):
    """Print to stderr for logging (stdout reserved for JSON output)"""
    print(message, file=_original_stderr, flush=True)


def transcribe_audio(
    audio_path: str,
    language: str = "vi",
    output_path: str = None,
    word_timestamps: bool = False,
    device: str = None,
    compute_type: str = None,
    model_size: str = "base"
) -> dict:
    """
    Transcribe audio using WhisperX
    
    Args:
        audio_path: Path to audio file (mp3, wav, etc.)
        language: Language code (en, vi, ja, etc.) or "auto" for detection
        output_path: Path to save transcription JSON
        word_timestamps: Whether to include word-level timestamps
        device: "cuda" or "cpu" (auto-detect if None)
        compute_type: "float16", "int8", or "float32"
        model_size: Whisper model size (tiny, base, small, medium, large)
    
    Returns:
        Dict with transcription segments
    """
    # Capture any stdout during transcription to prevent JSON pollution
    import io
    _stdout_capture = io.StringIO()
    _original = sys.stdout
    sys.stdout = _stdout_capture
    
    try:
        # Auto-detect device
        if device is None:
            device = "cuda" if torch.cuda.is_available() else "cpu"
        
        # Set compute type based on device
        if compute_type is None:
            compute_type = "float16" if device == "cuda" else "int8"
        
        log(f"[WhisperX] Using device: {device}, compute_type: {compute_type}, model: {model_size}")
        
        # Check if audio file exists
        if not os.path.exists(audio_path):
            return {
                "success": False,
                "error": f"Audio file not found: {audio_path}"
            }
        
        # Load audio
        log(f"[WhisperX] Loading audio: {audio_path}")
        audio = whisperx.load_audio(audio_path)
        
        # Load model
        log(f"[WhisperX] Loading model ({model_size})...")
        model = whisperx.load_model(model_size, device, compute_type=compute_type)
        
        # Transcribe
        log("[WhisperX] Transcribing audio...")
        lang = None if language == "auto" else language
        result = model.transcribe(audio, batch_size=16, language=lang)
        
        # Get detected language
        detected_language = result.get("language", language)
        log(f"[WhisperX] Detected language: {detected_language}")
        
        # Format segments (without word timestamps first)
        segments = []
        for seg in result.get("segments", []):
            segment_text = seg.get("text", "").strip()
            if segment_text:
                segments.append({
                    "startSec": round(seg.get("start", 0), 2),
                    "endSec": round(seg.get("end", 0), 2),
                    "text": segment_text
                })
        
        log(f"[WhisperX] Transcribed {len(segments)} segments")
        
        # Optionally add word-level timestamps via forced alignment
        word_timings = []
        if word_timestamps and len(segments) > 0:
            try:
                log("[WhisperX] Loading alignment model...")
                model_a, metadata = whisperx.load_align_model(
                    language_code=detected_language, 
                    device=device
                )
                
                log("[WhisperX] Performing forced alignment...")
                aligned_result = whisperx.align(
                    result["segments"], 
                    model_a, 
                    metadata, 
                    audio, 
                    device,
                    return_char_alignments=False
                )
                
                # Extract word timings
                for seg in aligned_result.get("segments", []):
                    for word_info in seg.get("words", []):
                        word = word_info.get("word", "").strip()
                        if not word:
                            continue
                        start = word_info.get("start", 0)
                        end = word_info.get("end", start)
                        score = word_info.get("score", 1.0)
                        word_timings.append({
                            "word": word,
                            "offset": round(start, 3),
                            "duration": round(end - start, 3),
                            "end": round(end, 3),
                            "score": round(score, 3) if score else 1.0
                        })
                
                log(f"[WhisperX] Aligned {len(word_timings)} words")
            except Exception as e:
                log(f"[WhisperX] Word alignment failed (continuing without): {e}")
        
        # Calculate duration
        duration = 0
        if segments:
            duration = segments[-1]["endSec"]
        
        # Build full text
        full_text = " ".join(seg["text"] for seg in segments)
        
        # Build result
        output = {
            "success": True,
            "segments": segments,
            "full_text": full_text,
            "language": detected_language,
            "duration": round(duration, 2),
            "segment_count": len(segments),
            "model": model_size,
            "device": device
        }
        
        if word_timestamps and word_timings:
            output["word_timings"] = word_timings
            output["word_count"] = len(word_timings)
        
        # Save to output file if path provided
        if output_path:
            os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(output, f, indent=2, ensure_ascii=False)
            log(f"[WhisperX] Saved transcription to: {output_path}")
            output["output_path"] = output_path
        
        return output
        
    except Exception as e:
        import traceback
        return {
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }
    finally:
        # Always restore stdout
        sys.stdout = _original


def main():
    """Main entry point - parse JSON input from command line"""
    if len(sys.argv) < 2:
        print(json.dumps({
            "success": False,
            "error": "Usage: python whisperx_transcribe.py '<json_input>'"
        }))
        sys.exit(1)
    
    try:
        input_data = json.loads(sys.argv[1])
    except json.JSONDecodeError as e:
        print(json.dumps({
            "success": False,
            "error": f"Invalid JSON input: {e}"
        }))
        sys.exit(1)
    
    # Extract parameters
    audio_path = input_data.get("audio_path")
    language = input_data.get("language", "vi")
    output_path = input_data.get("output_path")
    word_timestamps = input_data.get("word_timestamps", False)
    device = input_data.get("device")
    compute_type = input_data.get("compute_type")
    model_size = input_data.get("model_size", "base")
    
    if not audio_path:
        print(json.dumps({
            "success": False,
            "error": "audio_path is required"
        }))
        sys.exit(1)
    
    # Run transcription
    result = transcribe_audio(
        audio_path=audio_path,
        language=language,
        output_path=output_path,
        word_timestamps=word_timestamps,
        device=device,
        compute_type=compute_type,
        model_size=model_size
    )
    
    # Output result as JSON
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
