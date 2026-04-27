@echo off
REM Double-click: git push + build + GitHub Release + latest.json
REM May khach dung «Kiem tra cap nhat» trong app.

chcp 65001 >nul
setlocal EnableDelayedExpansion
cd /d "%~dp0"
set "TOOLFB_REPO=vanchien/ToolFB"

if exist "C:\Program Files\GitHub CLI\gh.exe" (
  set "PATH=C:\Program Files\GitHub CLI;%PATH%"
)
if exist "C:\Program Files\Git\cmd" (
  set "PATH=C:\Program Files\Git\cmd;%PATH%"
)

if exist ".venv\Scripts\python.exe" (
  set "PY=.venv\Scripts\python.exe"
) else (
  set "PY=python"
)

echo ============================================
echo   Dang ban len GitHub - ban moi cho moi nguoi
echo ============================================
echo.

"%PY%" tools\auto_github_publish.py --repo "%TOOLFB_REPO%" %*
set ERR=!errorlevel!

echo.
if !ERR! neq 0 (
  echo [LOI] Ma thoat: !ERR!
  echo Hay kiem tra: Git + GitHub CLI, da chay gh auth login, remote origin dung.
  pause
  exit /b !ERR!
)

echo [OK] Da publish xong. Cac may khac bam «Kiem tra cap nhat» de lay ban moi.
pause
exit /b 0
