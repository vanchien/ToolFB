#!/bin/bash

# Install WhisperX for audio transcription features (macOS/Linux)
# This script installs PyTorch (CPU) and WhisperX (~2-3GB download)

echo "═══════════════════════════════════════════════════════════════"
echo "  WhisperX Installation Script"
echo "  This will download ~2-3GB and may take 5-15 minutes"
echo "═══════════════════════════════════════════════════════════════"
echo ""

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Check if venv exists
if [ ! -d "venv" ]; then
    echo "❌ Virtual environment not found."
    echo "   Please run setup-venv.sh first."
    exit 1
fi

# Activate venv
source venv/bin/activate

echo "✅ Virtual environment activated"
echo ""

# Install PyTorch (CPU version for smaller size)
echo "📦 Installing PyTorch (CPU version)..."
echo "   This may take several minutes..."
pip install torch torchaudio --index-url https://download.pytorch.org/whl/cpu

if [ $? -ne 0 ]; then
    echo "❌ Failed to install PyTorch"
    exit 1
fi
echo "✅ PyTorch installed"
echo ""

# Install WhisperX
echo "📦 Installing WhisperX..."
pip install whisperx

if [ $? -ne 0 ]; then
    echo "❌ Failed to install WhisperX"
    exit 1
fi
echo "✅ WhisperX installed"
echo ""

# Verify installation
echo "🔍 Verifying installation..."
python -c "import torch; import whisperx; print('✅ WhisperX installed successfully!')"

if [ $? -ne 0 ]; then
    echo "❌ Verification failed"
    exit 1
fi

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  ✅ WhisperX Installation Complete!"
echo ""
echo "  You can now use transcription features in the app."
echo "  Restart the app to enable WhisperX."
echo "═══════════════════════════════════════════════════════════════"
echo ""
