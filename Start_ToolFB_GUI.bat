@echo off
setlocal
cd /d "%~dp0"

if exist ".venv\Scripts\python.exe" (
  ".venv\Scripts\python.exe" "main.py" --gui
) else (
  python "main.py" --gui
)
if errorlevel 1 (
  echo.
  echo [LOI] Khoi dong that bai ^(ma %ERRORLEVEL%^). Kiem tra: da tao .venv, cai requirements, va xem logs.
  pause
)

endlocal
