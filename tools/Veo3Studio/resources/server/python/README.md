# 📹 Video Analyzer Python Worker

Python worker for video analysis using PySceneDetect + Whisper STT.

## 🚀 Setup

```bash
# Create virtual environment
python3 -m venv venv

# Activate venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

## 📦 Dependencies

- **yt-dlp**: YouTube video download
- **PySceneDetect**: Scene detection
- **opencv-python**: Video processing
- **openai-whisper**: Speech-to-text (base model ~140MB)
- **numpy**: Numerical processing

## 🎯 Usage

```bash
# Activate venv first
source venv/bin/activate

# Analyze YouTube video
python video_analyzer.py "https://www.youtube.com/watch?v=..." vi

# Analyze local video file
python video_analyzer.py "/path/to/video.mp4" en

# Auto-detect language
python video_analyzer.py "video.mp4" auto
```

## 📤 Output

JSON output to stdout:

```json
{
  "video": {
    "sourceUrl": "https://youtube.com/...",
    "title": "Video Title",
    "durationSec": 120.5
  },
  "scenes": [
    {
      "id": "scene_001",
      "index": 1,
      "startSec": 0.0,
      "endSec": 7.8,
      "keyframePath": "/Users/.../video-temp/keyframe_scene_001.jpg",
      "transcriptSegments": [
        { "startSec": 0.5, "endSec": 3.2, "text": "Hello world" }
      ],
      "transcriptText": "Hello world"
    }
  ]
}
```

## 🔧 System Requirements

- Python 3.9+
- ffmpeg (in PATH)
- ffprobe (in PATH)
- ~2GB disk space for Whisper base model
- 8GB+ RAM recommended

## 📂 Temp Storage

- **Keyframes**: `~/Library/Application Support/EgTools/video-temp/`
- **Downloaded videos**: `/tmp/video_analyzer_*/`
- **Audio extraction**: `/tmp/*.wav` (auto-cleaned)

## ⚙️ Configuration

### Whisper Model Size

Current: `base` (fast, ~140MB)

Options:
- `tiny`: 39M params, ~75MB (fastest, least accurate)
- `base`: 74M params, ~140MB (default, balanced) ⭐
- `small`: 244M params, ~460MB (more accurate)
- `medium`: 769M params, ~1.4GB (high accuracy)
- `large`: 1550M params, ~3GB (best accuracy, slowest)

To change: Edit `video_analyzer.py` line ~172:
```python
model = whisper.load_model("base")  # Change to "small", "medium", etc.
```

### Scene Detection Threshold

Current: `27.0` (default)

- Lower (e.g., 20): More scenes detected (more sensitive)
- Higher (e.g., 35): Fewer scenes detected (less sensitive)

To change: Edit `video_analyzer.py` line ~72:
```python
def detect_scenes(video_path: str, threshold: float = 27.0):
```

## 🐛 Troubleshooting

### "ModuleNotFoundError"
```bash
# Make sure venv is activated
source venv/bin/activate

# Reinstall dependencies
pip install -r requirements.txt
```

### "ffmpeg not found"
```bash
# Install ffmpeg via Homebrew (macOS)
brew install ffmpeg

# Verify installation
which ffmpeg
which ffprobe
```

### "Whisper model download stuck"
Whisper models are auto-downloaded on first use to `~/.cache/whisper/`.
Check your internet connection and disk space.

### "Out of memory"
- Use `tiny` or `base` model instead of `large`
- Close other applications
- Ensure at least 4GB free RAM

## 📊 Performance

### Whisper Base Model (Apple M1, 8GB RAM)

| Video Length | Processing Time | RAM Usage |
|--------------|-----------------|-----------|
| 1 minute     | ~10 seconds     | ~1.5 GB   |
| 5 minutes    | ~45 seconds     | ~1.8 GB   |
| 10 minutes   | ~90 seconds     | ~2.0 GB   |

*Scene detection and keyframe extraction add ~5-10 seconds overhead*

## 🔗 Integration

This worker is called by `videoAnalysis.service.ts` (Node.js backend):

```typescript
const proc = spawn('python3', [pythonScriptPath, videoSource, languageHint]);
```

Logs are sent to stderr, JSON output to stdout.

## 📝 License

Part of EgTools project.

