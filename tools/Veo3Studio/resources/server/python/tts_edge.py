#!/usr/bin/env python3
"""
Edge TTS Worker for Node.js
Uses Microsoft Edge's online TTS service via edge-tts library

Usage: python tts_edge.py '<json_input>'
Input JSON: {
    "text": "Text to synthesize",
    "voice": "vi-VN-HoaiMyNeural",
    "output_path": "/path/to/output.mp3",
    "rate": "+0%",
    "volume": "+0%",
    "pitch": "+0Hz",
    "generate_subtitles": false
}
Output: JSON to stdout
"""

import sys
import json
import asyncio
import os

try:
    import edge_tts
except ImportError:
    print(json.dumps({
        "success": False,
        "error": "edge-tts not installed. Run: pip install edge-tts"
    }))
    sys.exit(1)


async def synthesize(
    text: str,
    voice: str,
    output_path: str,
    rate: str = "+0%",
    volume: str = "+0%",
    pitch: str = "+0Hz",
    generate_subtitles: bool = False
) -> dict:
    """
    Synthesize text to speech using Edge TTS
    
    Args:
        text: Text to synthesize
        voice: Voice ID (e.g., "vi-VN-HoaiMyNeural")
        output_path: Path to save MP3 file
        rate: Speed adjustment (e.g., "+10%", "-20%")
        volume: Volume adjustment (e.g., "+0%")
        pitch: Pitch adjustment (e.g., "+5Hz")
        generate_subtitles: Whether to generate SRT file and word timings
    
    Returns:
        Dict with success status and paths
    """
    try:
        # Ensure output directory exists
        output_dir = os.path.dirname(output_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        
        # Create communicate instance
        communicate = edge_tts.Communicate(
            text=text,
            voice=voice,
            rate=rate,
            volume=volume,
            pitch=pitch
        )
        
        if generate_subtitles:
            # Stream mode to capture word boundaries for subtitles
            submaker = edge_tts.SubMaker()
            word_timings = []  # Store word-level timing for highlighting
            sentence_boundaries = []  # Collect ALL sentence boundaries
            
            with open(output_path, "wb") as audio_file:
                async for chunk in communicate.stream():
                    if chunk["type"] == "audio":
                        audio_file.write(chunk["data"])
                    elif chunk["type"] == "WordBoundary":
                        submaker.feed(chunk)
                        # Extract word timing info
                        word_timings.append({
                            "word": chunk.get("text", ""),
                            "offset": chunk.get("offset", 0) / 10000000,  # Convert to seconds
                            "duration": chunk.get("duration", 0) / 10000000,
                        })
                    elif chunk["type"] == "SentenceBoundary":
                        submaker.feed(chunk)
                        sentence_boundaries.append({
                            "offset": chunk.get("offset", 0) / 10000000,
                            "duration": chunk.get("duration", 0) / 10000000,
                            "text": chunk.get("text", "")
                        })
            
            # If no word boundaries, estimate from ALL sentence boundaries
            if not word_timings and sentence_boundaries:
                words = text.split()
                word_index = 0
                
                for sentence in sentence_boundaries:
                    sentence_words = sentence["text"].split()
                    if not sentence_words:
                        continue
                    
                    total_chars = sum(len(w) for w in sentence_words)
                    if total_chars == 0:
                        continue
                        
                    current_offset = sentence["offset"]
                    sentence_duration = sentence["duration"]
                    
                    for sw in sentence_words:
                        if word_index < len(words):
                            word_duration = (len(sw) / total_chars) * sentence_duration
                            word_timings.append({
                                "word": words[word_index],
                                "offset": round(current_offset, 3),
                                "duration": round(word_duration, 3),
                            })
                            current_offset += word_duration
                            word_index += 1
                
                # Handle any remaining words (if text has more words than sentences covered)
                if word_index < len(words) and sentence_boundaries:
                    last_sentence = sentence_boundaries[-1]
                    remaining_offset = last_sentence["offset"] + last_sentence["duration"]
                    remaining_words = words[word_index:]
                    avg_word_duration = 0.3  # Fallback duration per word
                    
                    for rw in remaining_words:
                        word_timings.append({
                            "word": rw,
                            "offset": round(remaining_offset, 3),
                            "duration": avg_word_duration,
                        })
                        remaining_offset += avg_word_duration
            
            # Save subtitles (SRT format)
            subtitles_path = output_path.replace(".mp3", ".srt")
            srt_content = submaker.get_srt()
            
            with open(subtitles_path, "w", encoding="utf-8") as srt_file:
                srt_file.write(srt_content)
            
            # Save word timings as JSON for word-level highlighting
            word_timings_path = output_path.replace(".mp3", "_words.json")
            with open(word_timings_path, "w", encoding="utf-8") as words_file:
                json.dump(word_timings, words_file, indent=2)
            
            # Calculate duration
            duration = 0
            if word_timings:
                last_word = word_timings[-1]
                duration = last_word["offset"] + last_word["duration"]
            elif sentence_boundaries:
                last_sentence = sentence_boundaries[-1]
                duration = last_sentence["offset"] + last_sentence["duration"]
            else:
                # Fallback to file size estimation
                file_size = os.path.getsize(output_path)
                duration = file_size / 6000
            
            return {
                "success": True,
                "audio_path": output_path,
                "subtitles_path": subtitles_path,
                "word_timings_path": word_timings_path,
                "duration": round(duration, 2),
                "word_count": len(word_timings)
            }
        else:
            # Simple save mode (faster)
            await communicate.save(output_path)
            
            # Get audio duration from file size (approximate)
            file_size = os.path.getsize(output_path)
            duration = file_size / 6000
            
            return {
                "success": True,
                "audio_path": output_path,
                "duration": round(duration, 2)
            }
            
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


async def list_voices(language: str = None) -> dict:
    """
    List available voices
    
    Args:
        language: Filter by language code (e.g., "vi", "en")
    
    Returns:
        Dict with voices list
    """
    try:
        voices = await edge_tts.VoicesManager.create()
        
        if language:
            voice_list = voices.find(Language=language)
        else:
            voice_list = voices.voices
        
        return {
            "success": True,
            "voices": [
                {
                    "id": v["ShortName"],
                    "name": v["FriendlyName"],
                    "locale": v["Locale"],
                    "language": v["Locale"].split("-")[0],
                    "gender": v["Gender"].lower()
                }
                for v in voice_list
            ]
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


def main():
    if len(sys.argv) < 2:
        print(json.dumps({
            "success": False,
            "error": "Missing JSON input argument"
        }))
        sys.exit(1)
    
    try:
        input_data = json.loads(sys.argv[1])
    except json.JSONDecodeError as e:
        print(json.dumps({
            "success": False,
            "error": f"Invalid JSON input: {str(e)}"
        }))
        sys.exit(1)
    
    # Determine action
    action = input_data.get("action", "synthesize")
    
    if action == "list_voices":
        result = asyncio.run(list_voices(input_data.get("language")))
    elif action == "synthesize":
        # Validate required fields
        required = ["text", "voice", "output_path"]
        for field in required:
            if field not in input_data:
                print(json.dumps({
                    "success": False,
                    "error": f"Missing required field: {field}"
                }))
                sys.exit(1)
        
        result = asyncio.run(synthesize(
            text=input_data["text"],
            voice=input_data["voice"],
            output_path=input_data["output_path"],
            rate=input_data.get("rate", "+0%"),
            volume=input_data.get("volume", "+0%"),
            pitch=input_data.get("pitch", "+0Hz"),
            generate_subtitles=input_data.get("generate_subtitles", False)
        ))
    else:
        result = {
            "success": False,
            "error": f"Unknown action: {action}"
        }
    
    print(json.dumps(result))
    sys.exit(0 if result.get("success") else 1)


if __name__ == "__main__":
    main()
