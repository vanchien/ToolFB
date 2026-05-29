@echo off
setlocal
cd /d "%~dp0\.."
echo === ToolFB - Cai dat lan dau (Windows) ===
where python >nul 2>&1
if errorlevel 1 (
  echo Khong tim thay python. Cai Python 3.10+ tu python.org roi chay lai.
  exit /b 1
)
if not exist ".venv\Scripts\python.exe" (
  echo Tao virtualenv .venv ...
  python -m venv .venv
)
call ".venv\Scripts\python.exe" -m pip install --upgrade pip
call ".venv\Scripts\pip.exe" install -r requirements.txt
echo Cai trinh duyet Playwright (Firefox) ...
call ".venv\Scripts\python.exe" -m playwright install firefox
echo Bootstrap config/data ...
call ".venv\Scripts\python.exe" -c "from src.utils.first_run_bootstrap import bootstrap_all; bootstrap_all()"
echo.
echo Xong. Chay: Start_ToolFB_GUI.bat  hoac  .venv\Scripts\python.exe main.py --gui
endlocal
