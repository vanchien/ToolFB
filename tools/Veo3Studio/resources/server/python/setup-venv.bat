@echo off
REM Setup Python virtual environment for video analysis (Windows)
REM This script creates venv and installs all required dependencies

echo 🐍 Setting up Python virtual environment...

REM Check Python version
where python >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo ❌ Python not found. Please install Python 3.9+ first.
    echo    Download from: https://www.python.org/downloads/
    pause
    exit /b 1
)

for /f "tokens=2" %%i in ('python --version 2^>^&1') do set PYTHON_VERSION=%%i
echo ✅ Found Python %PYTHON_VERSION%

REM Get script directory
set SCRIPT_DIR=%~dp0
cd /d %SCRIPT_DIR%

REM Create venv if not exists
if not exist "venv" (
    echo 📦 Creating virtual environment...
    python -m venv venv
    if %ERRORLEVEL% neq 0 (
        echo ❌ Failed to create virtual environment
        pause
        exit /b 1
    )
    echo ✅ Virtual environment created
) else (
    echo ✅ Virtual environment already exists
)

REM Activate venv
call venv\Scripts\activate.bat
if %ERRORLEVEL% neq 0 (
    echo ❌ Failed to activate virtual environment
    pause
    exit /b 1
)

REM Upgrade pip
echo ⬆️  Upgrading pip...
python -m pip install --upgrade pip

REM Install dependencies
echo 📦 Installing Python dependencies...
if exist "requirements.txt" (
    pip install -r requirements.txt
    if %ERRORLEVEL% neq 0 (
        echo ⚠️  Some dependencies may have failed to install
    ) else (
        echo ✅ Dependencies installed
    )
) else (
    echo ⚠️  requirements.txt not found, installing basic dependencies...
    pip install yt-dlp scenedetect opencv-python numpy edge-tts pillow requests google-genai
)

REM Verify installation
echo 🔍 Verifying installation...
python -c "import yt_dlp; import cv2; import numpy; import edge_tts; print('✅ Core dependencies installed successfully')"
if %ERRORLEVEL% neq 0 (
    echo ⚠️  Some dependencies may not be fully installed
) else (
    echo ✅ All core dependencies verified
)

echo.
echo ✅ Python environment setup complete!
echo.
echo ═══════════════════════════════════════════════════════════════
echo   OPTIONAL: Install WhisperX for transcription features
echo   This requires ~2-3GB download and 5-15 minutes
echo.
echo   To install WhisperX manually:
echo     pip install torch torchaudio --index-url https://download.pytorch.org/whl/cpu
echo     pip install whisperx
echo.
echo   Or use the app's "Install WhisperX" button in Settings
echo ═══════════════════════════════════════════════════════════════
