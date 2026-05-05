#!/usr/bin/env python3
"""
Whisper Transcription Script
Uses WhisperX for fast transcription

Usage: python whisper_transcribe.py '<json_input>'
Input JSON: {
    "audio_path": "/path/to/audio.mp3",
    "language": "en",
    "model": "base",
    "device": "cuda" (optional)
}

Output: JSON with segments and full text
"""

import sys
import json
import os
import warnings
import logging
import contextlib

# Configure logging to stderr
logging.basicConfig(level=logging.INFO, stream=sys.stderr, format='%(message)s')
logger = logging.getLogger(__name__)

# Suppress warnings
warnings.filterwarnings("ignore")

@contextlib.contextmanager
def suppress_stdout():
    """Context manager to suppress stdout during noisy imports/functions"""
    with open(os.devnull, "w") as devnull:
        old_stdout = sys.stdout
        sys.stdout = devnull
        try:
            yield
        finally:
            sys.stdout = old_stdout

# Check for required packages
try:
    with suppress_stdout():
        import torch
        
        import torch
        import omegaconf
        import omegaconf.listconfig
        import omegaconf.dictconfig
        import omegaconf.base
        
        # PyTorch 2.6+ security: Register Safe Globals
        # We need to whitelist all Omegaconf classes used in the checkpoint
        try:
            torch.serialization.add_safe_globals([
                omegaconf.listconfig.ListConfig,
                omegaconf.dictconfig.DictConfig,
                omegaconf.base.ContainerMetadata,
                omegaconf.base.Node
            ])
        except (AttributeError, ImportError):
             # Older torch versions don't have this function
            pass

        # Also attempt to monkey-patch torch.load as a fallback for other unknown globals
        if hasattr(torch, 'load'):
            _original_load = torch.load
            def _patched_load(*args, **kwargs):
                if 'weights_only' not in kwargs:
                    try:
                        return _original_load(*args, weights_only=False, **kwargs)
                    except TypeError:
                        return _original_load(*args, **kwargs)
                return _original_load(*args, **kwargs)
            torch.load = _patched_load

        import whisperx
except ImportError as e:
    # Print error to actual stdout so it can be parsed (or stderr if preferred, but service expects JSON on stdout)
    # actually service catches non-zero exit, so we should print json to stdout if possible or exit 1
    # We'll print to stderr for logs and exit 1
    logger.error(json.dumps({
        "success": False,
        "error": f"Required package not installed: {e}. Run: pip install whisperx torch"
    }))
    sys.exit(1)


def transcribe_audio(
    audio_path: str,
    language: str = "en",
    model_name: str = "base",
    device: str = None,
    compute_type: str = None
) -> dict:
    try:
        # Auto-detect device
        if device is None:
            device = "cuda" if torch.cuda.is_available() else "cpu"
        
        # Set compute type based on device
        if compute_type is None:
            compute_type = "float16" if device == "cuda" else "int8"
        
        logger.info(f"[Whisper] Using device: {device}, compute_type: {compute_type}, model: {model_name}")
        
        # Load audio
        if not os.path.exists(audio_path):
            raise FileNotFoundError(f"Audio file not found: {audio_path}")
            
        logger.info(f"[Whisper] Loading audio: {audio_path}")
        audio = whisperx.load_audio(audio_path)
        
        # Load model (suppress stdout as load_model is noisy)
        logger.info(f"[Whisper] Loading model: {model_name}...")
        with suppress_stdout():
            model = whisperx.load_model(model_name, device, compute_type=compute_type)
        
        # Transcribe (suppress stdout as transcribe is noisy)
        logger.info(f"[Whisper] Transcribing (language: {language})...")
        with suppress_stdout():
            result = model.transcribe(audio, batch_size=16, language=language)
        
        # Format segments
        segments = []
        full_text = ""
        
        for segment in result.get("segments", []):
            text = segment.get("text", "").strip()
            if text:
                segments.append({
                    "start": segment.get("start"),
                    "end": segment.get("end"),
                    "text": text
                })
                full_text += text + " "
        
        # Estimate duration from last segment
        duration = 0
        if segments:
            duration = segments[-1]["end"]
            
        return {
            "success": True,
            "segments": segments,
            "fullText": full_text.strip(),
            "duration": duration,
            "language": language,
            "model": model_name,
            "device": device
        }
        
    except Exception as e:
        import traceback
        # Return error dict (caller will print as JSON)
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
            "error": "Usage: python whisper_transcribe.py '<json_input>'"
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
    language = input_data.get("language", "en")
    model_name = input_data.get("model", "base")
    device = input_data.get("device")
    
    if not audio_path:
        print(json.dumps({
            "success": False,
            "error": "audio_path is required"
        }))
        sys.exit(1)
    
    # Run transcription
    # We intentionally do NOT suppress stdout here if transcribe_audio prints something unexpected, 
    # but transcribe_audio is controlled now.
    result = transcribe_audio(
        audio_path=audio_path,
        language=language,
        model_name=model_name,
        device=device
    )
    
    # Output result as JSON - this is the ONLY thing that should be on stdout
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
