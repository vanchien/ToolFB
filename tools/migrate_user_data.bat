@echo off
setlocal
cd /d "%~dp0\.."

echo.
echo === ToolFB Data Migration ===
echo Nguon: thu muc ToolFB cu
echo Dich : thu muc ToolFB moi (hoac thu muc exe_gui)
echo.

set /p OLD_DIR=Nhap duong dan THU MUC CU: 
set /p NEW_DIR=Nhap duong dan THU MUC MOI: 

if "%OLD_DIR%"=="" (
  echo [LOI] Chua nhap thu muc cu.
  pause
  exit /b 1
)
if "%NEW_DIR%"=="" (
  echo [LOI] Chua nhap thu muc moi.
  pause
  exit /b 1
)

set "PY=.venv\Scripts\python.exe"
if exist "%PY%" (
  "%PY%" "tools\migrate_user_data.py" --from "%OLD_DIR%" --to "%NEW_DIR%"
) else (
  python "tools\migrate_user_data.py" --from "%OLD_DIR%" --to "%NEW_DIR%"
)

echo.
echo Hoan tat. Bam phim bat ky de dong...
pause >nul
endlocal
