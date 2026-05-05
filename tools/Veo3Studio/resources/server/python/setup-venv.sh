#!/bin/bash

# Setup Python virtual environment for video analysis
# This script creates venv and installs all required dependencies

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "🐍 Setting up Python virtual environment..."

# Check Python version
if ! command -v python3 &> /dev/null; then
    echo "❌ Python3 not found. Please install Python 3.9+ first."
    exit 1
fi

PYTHON_VERSION=$(python3 --version | cut -d' ' -f2 | cut -d'.' -f1,2)
echo "✅ Found Python $PYTHON_VERSION"

# Create venv if not exists
if [ ! -d "venv" ]; then
    echo "📦 Creating virtual environment..."
    python3 -m venv venv
    echo "✅ Virtual environment created"
else
    echo "✅ Virtual environment already exists"
fi

# Activate venv
if [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
elif [ -f "venv/Scripts/activate" ]; then
    source venv/Scripts/activate
else
    echo "❌ Cannot find venv activation script"
    exit 1
fi

# Upgrade pip
echo "⬆️  Upgrading pip..."
pip install --upgrade pip

# Install dependencies
echo "📦 Installing Python dependencies..."
if [ -f "requirements.txt" ]; then
    pip install -r requirements.txt
    echo "✅ Dependencies installed"
else
    echo "⚠️  requirements.txt not found, installing basic dependencies..."
    pip install yt-dlp scenedetect[opencv] opencv-python numpy edge-tts pillow requests google-genai
fi

# Verify installation
echo "🔍 Verifying installation..."
python -c "import yt_dlp; import scenedetect; import cv2; import numpy; import edge_tts; print('✅ All dependencies installed successfully')" || {
    echo "⚠️ Some optional dependencies may not be installed"
}

echo ""
echo "✅ Python environment setup complete!"
echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  OPTIONAL: Install WhisperX for transcription features"
echo "  This requires ~2-3GB download and 5-15 minutes"
echo ""
echo "  To install WhisperX manually:"
echo "    pip install torch torchaudio --index-url https://download.pytorch.org/whl/cpu"
echo "    pip install whisperx"
echo ""
echo "  Or use the app's 'Install WhisperX' button in Settings"
echo "═══════════════════════════════════════════════════════════════"
echo ""
echo "To activate venv manually:"
echo "  source venv/bin/activate  # macOS/Linux"
echo "  venv\\Scripts\\activate     # Windows"
echo ""