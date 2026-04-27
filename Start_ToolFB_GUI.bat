@echo off
setlocal
cd /d "%~dp0"

if exist ".venv\Scripts\python.exe" (
  ".venv\Scripts\python.exe" "main.py" --gui
) else (
  python "main.py" --gui
)

endlocal
