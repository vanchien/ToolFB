@echo off
setlocal
cd /d "%~dp0"
if not exist ".venv\Scripts\python.exe" (
  echo Chua co .venv — chay scripts\setup_windows.bat lan dau.
  pause
  exit /b 1
)
".venv\Scripts\python.exe" "main.py" --gui
endlocal
