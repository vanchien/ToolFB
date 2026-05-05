#!/usr/bin/env python3
"""
WhisperX Word-Level Alignment Script
Uses forced alignment with wav2vec2 for highly accurate word timestamps

Usage: python whisperx_align.py '<json_input>'
Input JSON: {
    "audio_path": "/path/to/audio.mp3",
    "text": "The transcript text to align",
    "language": "en",
    "output_path": "/path/to/output_words.json"
}

Output: Word-level timestamps JSON file

Workflow:
1. Audio → Whisper (or provided transcript)
2. Forced Alignment with wav2vec2 model
3. Word-level timestamps (very accurate)
"""

import sys
import json
import os

# Check for required packages
try:
    import torch
    import whisperx
except ImportError as e:
    print(json.dumps({
        "success": False,
        "error": f"Required package not installed: {e}. Run: pip install whisperx torch"
    }))
    sys.exit(1)


def align_audio(
    audio_path: str,
    text: str = None,
    language: str = "en",
    output_path: str = None,
    device: str = None,
    compute_type: str = None
) -> dict:
    """
    Perform forced alignment on audio using WhisperX
    
    Args:
        audio_path: Path to audio file (mp3, wav, etc.)
        text: Optional pre-existing transcript (if None, will transcribe first)
        language: Language code (en, vi, ja, etc.)
        output_path: Path to save word timings JSON
        device: "cuda" or "cpu" (auto-detect if None)
        compute_type: "float16", "int8", or "float32"
    
    Returns:
        Dict with word-level timestamps
    """
    try:
        # Auto-detect device
        if device is None:
            device = "cuda" if torch.cuda.is_available() else "cpu"
        
        # Set compute type based on device
        if compute_type is None:
            compute_type = "float16" if device == "cuda" else "int8"
        
        print(f"[WhisperX] Using device: {device}, compute_type: {compute_type}", file=sys.stderr)
        
        # Load audio
        print(f"[WhisperX] Loading audio: {audio_path}", file=sys.stderr)
        audio = whisperx.load_audio(audio_path)
        
        # Step 1: Transcribe if no text provided
        if text is None or text.strip() == "":
            print("[WhisperX] Transcribing audio...", file=sys.stderr)
            model = whisperx.load_model("base", device, compute_type=compute_type)
            result = model.transcribe(audio, batch_size=16, language=language)
        else:
            # Use provided text - create segments from it
            print("[WhisperX] Using provided transcript", file=sys.stderr)
            # Create a simple result structure with the full text as one segment
            result = {
                "segments": [{"text": text.strip(), "start": 0, "end": 0}],
                "language": language
            }
        
        # Step 2: Load alignment model
        print(f"[WhisperX] Loading alignment model for language: {language}", file=sys.stderr)
        model_a, metadata = whisperx.load_align_model(
            language_code=language, 
            device=device
        )
        
        # Step 3: Align (forced alignment with wav2vec2)
        print("[WhisperX] Performing forced alignment...", file=sys.stderr)
        result = whisperx.align(
            result["segments"], 
            model_a, 
            metadata, 
            audio, 
            device,
            return_char_alignments=False
        )
        
        # Extract word-level timings
        word_timings = []
        
        for segment in result.get("segments", []):
            for word_info in segment.get("words", []):
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
        
        # Calculate total duration
        duration = 0
        if word_timings:
            last_word = word_timings[-1]
            duration = last_word["end"]
        
        # Save to output file if path provided
        if output_path:
            os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(word_timings, f, indent=2, ensure_ascii=False)
            print(f"[WhisperX] Saved word timings to: {output_path}", file=sys.stderr)
        
        return {
            "success": True,
            "word_timings": word_timings,
            "word_count": len(word_timings),
            "duration": round(duration, 3),
            "output_path": output_path,
            "language": language,
            "device": device
        }
        
    except Exception as e:
        import traceback
        return {
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }


def main():
    """Main entry point - parse JSON input from command line"""
    if len(sys.argv) < 2:
        print(json.dumps({
            "success": False,
            "error": "Usage: python whisperx_align.py '<json_input>'"
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
    text = input_data.get("text")
    language = input_data.get("language", "en")
    output_path = input_data.get("output_path")
    device = input_data.get("device")
    compute_type = input_data.get("compute_type")
    
    if not audio_path:
        print(json.dumps({
            "success": False,
            "error": "audio_path is required"
        }))
        sys.exit(1)
    
    # Run alignment
    result = align_audio(
        audio_path=audio_path,
        text=text,
        language=language,
        output_path=output_path,
        device=device,
        compute_type=compute_type
    )
    
    # Output result as JSON
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
