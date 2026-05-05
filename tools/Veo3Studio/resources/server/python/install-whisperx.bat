@echo off
REM Install WhisperX for audio transcription features (Windows)
REM This script installs PyTorch (CPU) and WhisperX (~2-3GB download)

echo ═══════════════════════════════════════════════════════════════
echo   WhisperX Installation Script
echo   This will download ~2-3GB and may take 5-15 minutes
echo ═══════════════════════════════════════════════════════════════
echo.

REM Get script directory
set SCRIPT_DIR=%~dp0
cd /d %SCRIPT_DIR%

REM Check if venv exists
if not exist "venv" (
    echo ❌ Virtual environment not found.
    echo    Please run setup-venv.bat first.
    pause
    exit /b 1
)

REM Activate venv
call venv\Scripts\activate.bat
if %ERRORLEVEL% neq 0 (
    echo ❌ Failed to activate virtual environment
    pause
    exit /b 1
)

echo ✅ Virtual environment activated
echo.

REM Install PyTorch (CPU version for smaller size)
echo 📦 Installing PyTorch (CPU version)...
echo    This may take several minutes...
pip install torch torchaudio --index-url https://download.pytorch.org/whl/cpu
if %ERRORLEVEL% neq 0 (
    echo ❌ Failed to install PyTorch
    pause
    exit /b 1
)
echo ✅ PyTorch installed
echo.

REM Install WhisperX
echo 📦 Installing WhisperX...
pip install whisperx
if %ERRORLEVEL% neq 0 (
    echo ❌ Failed to install WhisperX
    pause
    exit /b 1
)
echo ✅ WhisperX installed
echo.

REM Verify installation
echo 🔍 Verifying installation...
python -c "import torch; import whisperx; print('✅ WhisperX installed successfully!')"
if %ERRORLEVEL% neq 0 (
    echo ❌ Verification failed
    pause
    exit /b 1
)

echo.
echo ═══════════════════════════════════════════════════════════════
echo   ✅ WhisperX Installation Complete!
echo.
echo   You can now use transcription features in the app.
echo   Restart the app to enable WhisperX.
echo ═══════════════════════════════════════════════════════════════
echo.
pause
